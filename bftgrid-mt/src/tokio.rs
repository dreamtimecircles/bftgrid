use std::{
    collections::HashMap,
    io, mem,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use bftgrid_core::{
    ActorControl, ActorMsg, ActorRef, ActorSystem, Joinable, P2PNetwork, P2PNetworkError,
    P2PNetworkResult, TypedHandler, UntypedHandler,
};
use futures::future;
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender},
    task::JoinHandle as TokioJoinHandle,
};

use crate::notify_close;

#[derive(Debug)]
pub struct TokioActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    actor_system: TokioActorSystem,
    tx: TUnboundedSender<MsgT>,
    handler_tx: TUnboundedSender<Arc<Mutex<HandlerT>>>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<MsgT, HandlerT> Joinable<()> for TokioActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn join(self) {
        let (close_mutex, cvar) = &*self.close_cond;
        let mut closed = close_mutex.lock().unwrap();
        while !*closed {
            closed = cvar.wait(closed).unwrap();
        }
    }

    fn is_finished(&mut self) -> bool {
        let (closed_mutex, _) = &*self.close_cond;
        *closed_mutex.lock().unwrap()
    }
}

impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for TokioActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.tx.clone();
        let t = self.actor_system.spawn(async move {
            if let Some(delay_duration) = delay {
                log::debug!("Delaying send by {:?}", delay_duration);
                tokio::time::sleep(delay_duration).await;
            }
            sender.send(message).ok().unwrap()
        });
        self.actor_system.tasks.lock().unwrap().push(t);
    }

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>> {
        Box::new(TokioActor {
            actor_system: self.actor_system.clone(),
            tx: self.tx.clone(),
            handler_tx: self.handler_tx.clone(),
            close_cond: self.close_cond.clone(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct TokioActorSystem {
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    tasks: Arc<Mutex<Vec<TokioJoinHandle<()>>>>,
}

impl TokioActorSystem {
    pub fn new() -> Self {
        TokioActorSystem {
            tasks: Default::default(),
            runtime: None,
        }
    }

    fn spawn<F>(&mut self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: core::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn(&mut self.runtime, future)
    }
}

fn spawn<F>(
    runtime: &mut Option<Arc<tokio::runtime::Runtime>>,
    future: F,
) -> tokio::task::JoinHandle<F::Output>
where
    F: core::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            log::debug!("Current Tokio runtime found");
            handle.spawn(future)
        }
        Err(_) => match runtime {
            Some(r) => {
                log::debug!("No current Tokio runtime available but one dedicated to the actor system was already created");
                r.spawn(future)
            }
            None => {
                log::debug!("No current Tokio runtime and not created a dedicated one for the actor system yet, creating it");
                let r = tokio::runtime::Runtime::new().unwrap();
                let res = r.spawn(future);
                *runtime = Some(Arc::new(r));
                res
            }
        },
    }
}

impl Default for TokioActorSystem {
    fn default() -> Self {
        TokioActorSystem::new()
    }
}

impl TokioActorSystem {
    pub async fn join(self) {
        let mut tasks = vec![];
        {
            let mut locked_tasks = self.tasks.lock().unwrap();
            mem::swap(&mut *locked_tasks, &mut tasks);
            // Drop the lock before waiting for all tasks to finish, else the actor system will deadlock on spawns
        }
        log::info!("Tokio actor system joining {} tasks", tasks.len());
        for t in tasks {
            t.await.unwrap();
        }
    }

    pub fn is_finished(&mut self) -> bool {
        self.tasks
            .lock()
            .unwrap()
            .iter_mut()
            .all(|h| h.is_finished())
    }
}

impl ActorSystem for TokioActorSystem {
    type ActorRefT<MsgT, HandlerT> = TokioActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        name: impl Into<String>,
        node_id: impl Into<String>,
    ) -> TokioActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (handler_tx, mut handler_rx) = tmpsc::unbounded_channel::<Arc<Mutex<HandlerT>>>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        let actor_name = name.into();
        let actor_node_id = node_id.into();
        let t = self.spawn(async move {
            let mut current_handler = handler_rx.recv().await.unwrap();
            log::debug!("Async actor {} on node {} started", actor_name, actor_node_id);
            loop {
                if let Ok(new_handler) = handler_rx.try_recv() {
                    log::debug!("Async actor {} on node {}: new handler received", actor_name, actor_node_id);
                    current_handler = new_handler;
                }
                match rx.recv().await {
                    None => {
                        log::info!("Async actor {} on node {}: shutting down due to message receive channel having being closed", actor_name, actor_node_id);
                        rx.close();
                        handler_rx.close();
                        notify_close(close_cond2);
                        return;
                    }
                    Some(m) => {
                        if let Some(control) = current_handler.lock().unwrap().receive(m) {
                            match control {
                                ActorControl::Exit() => {
                                    log::info!("Async actor {} on node {}: closing requested by handler, shutting it down", actor_name, actor_node_id);
                                    rx.close();
                                    handler_rx.close();
                                    notify_close(close_cond2);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        });
        self.tasks.lock().unwrap().push(t);
        TokioActor {
            actor_system: self.clone(),
            tx,
            handler_tx,
            close_cond,
        }
    }

    fn set_handler<MsgT, HandlerT>(
        &mut self,
        actor_ref: &mut TokioActor<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
    {
        actor_ref
            .handler_tx
            .send(Arc::new(Mutex::new(handler)))
            .unwrap();
    }
}

pub struct TokioNetworkNode<UntypedHandlerT: UntypedHandler> {
    handler: UntypedHandlerT,
    socket: UdpSocket,
}

impl<UntypedHandlerT: UntypedHandler + 'static> TokioNetworkNode<UntypedHandlerT> {
    pub fn new(handler: UntypedHandlerT, socket: UdpSocket) -> P2PNetworkResult<Self> {
        Ok(TokioNetworkNode { handler, socket })
    }

    pub fn start<MsgT, DeT>(
        mut self,
        deserializer: DeT,
        buffer_size: usize,
    ) -> io::Result<TokioJoinHandle<()>>
    where
        MsgT: ActorMsg,
        DeT: Fn(&mut [u8]) -> P2PNetworkResult<MsgT> + Send + 'static,
    {
        let addr = self.socket.local_addr()?;
        log::error!("Async actor netork server {} starting", addr);
        Ok(tokio::spawn(async move {
            let mut buf = vec![0; buffer_size];
            loop {
                let valid_bytes = match self.socket.recv(&mut buf[..]).await {
                    Ok(bs) => bs,
                    Err(e) => {
                        log::warn!(
                            "Async actor netork server {}: datagram receive failed: {}",
                            addr,
                            e
                        );
                        continue;
                    }
                };
                let message = match deserializer(&mut buf[..valid_bytes]) {
                    Ok(m) => m,
                    Err(e) => {
                        log::warn!(
                            "Async actor netork server {}: deserialization failed: {:?}",
                            addr,
                            e
                        );
                        continue;
                    }
                };
                if let Some(ActorControl::Exit()) =
                    match self.handler.receive_untyped(Box::new(message)) {
                        Ok(m) => m,
                        Err(e) => {
                            log::warn!(
                                "Async actor netork server {}: handler receive failed: {:?}",
                                addr,
                                e
                            );
                            continue;
                        }
                    }
                {
                    break;
                }
            }
        }))
    }
}

#[derive(Debug, Clone)]
pub struct TokioP2PNetwork {
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    sockets: HashMap<String, P2PNetworkResult<Arc<UdpSocket>>>,
}

fn attempt_send_internal<MsgT, SerializerT>(
    runtime: &mut Option<Arc<tokio::runtime::Runtime>>,
    sockets: &HashMap<String, P2PNetworkResult<Arc<tokio::net::UdpSocket>>>,
    message: MsgT,
    serializer: &SerializerT,
    node: impl AsRef<str>,
) -> P2PNetworkResult<()>
where
    MsgT: ActorMsg,
    SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync,
{
    println!("Sending to {}", node.as_ref());
    let socket_handle = match sockets.get(node.as_ref()) {
        Some(s) => (*s).clone(),
        None => P2PNetworkResult::Err(P2PNetworkError::ActorNotFound(node.as_ref().to_owned())),
    }?;
    let serialized_message = serializer(message)?;
    spawn(runtime, async move {
        socket_handle.send(&serialized_message[..]).await
    });
    Ok(())
}

impl TokioP2PNetwork {
    pub async fn new(initial_peers: Vec<impl Into<String>>) -> Self {
        let initial_peer_addrs: Vec<String> = initial_peers.into_iter().map(|p| p.into()).collect(); // Consumed to produce result
        let initial_peer_addrs_clone = initial_peer_addrs.clone(); // Consumed by `connect`
        let sockets = initial_peer_addrs
            .into_iter()
            .zip(
                future::join_all(initial_peer_addrs_clone.into_iter().map(|p| {
                    tokio::runtime::Handle::current().spawn(async move {
                        let socket = UdpSocket::bind("localhost:0").await?;
                        socket.connect(p).await.map(|_| socket)
                    })
                }))
                .await
                .into_iter()
                .map(|rr| match rr {
                    Ok(r) => match r {
                        Ok(socket) => P2PNetworkResult::<Arc<UdpSocket>>::Ok(Arc::new(socket)),
                        Err(e) => P2PNetworkResult::<Arc<UdpSocket>>::Err(P2PNetworkError::Io(
                            Arc::new(e),
                        )),
                    },
                    Err(e) => {
                        P2PNetworkResult::<Arc<UdpSocket>>::Err(P2PNetworkError::Join(Arc::new(e)))
                    }
                }),
            )
            .collect();

        TokioP2PNetwork {
            runtime: None,
            sockets,
        }
    }
}

impl P2PNetwork for TokioP2PNetwork {
    fn attempt_send<MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        serializer: &SerializerT,
        node: impl AsRef<str>,
    ) -> P2PNetworkResult<()>
    where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync,
    {
        attempt_send_internal(&mut self.runtime, &self.sockets, message, serializer, node)
    }
}
