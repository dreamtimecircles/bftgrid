use std::{
    collections::HashMap,
    error::Error,
    mem,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use bftgrid_core::{
    AResult, ActorControl, ActorMsg, ActorRef, ActorSystem, Joinable, P2PNetwork, TypedHandler,
    UntypedHandlerBox,
};
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
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    actor_system: TokioActorSystem,
    tx: TUnboundedSender<MsgT>,
    handler_tx: TUnboundedSender<Arc<Mutex<HandlerT>>>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<MsgT, HandlerT> Joinable<()> for TokioActor<MsgT, HandlerT>
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
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
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.tx.clone();
        let t = self.actor_system.spawn(async move {
            if let Some(delay_duration) = delay {
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
        Ok(handle) => handle.spawn(future),
        Err(_) => match runtime {
            Some(r) => r.spawn(future),
            None => {
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
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        _name: impl Into<String>,
        _node_id: impl Into<String>,
    ) -> TokioActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (handler_tx, mut handler_rx) = tmpsc::unbounded_channel::<Arc<Mutex<HandlerT>>>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        let t = self.spawn(async move {
            let mut current_handler = handler_rx.recv().await.unwrap();
            loop {
                if let Ok(new_handler) = handler_rx.try_recv() {
                    current_handler = new_handler;
                }
                match rx.recv().await {
                    None => {
                        rx.close();
                        handler_rx.close();
                        notify_close(close_cond2);
                        return;
                    }
                    Some(m) => {
                        if let Some(control) = current_handler.lock().unwrap().receive(m) {
                            match control {
                                ActorControl::Exit() => {
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
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    {
        actor_ref
            .handler_tx
            .send(Arc::new(Mutex::new(handler)))
            .unwrap();
    }
}

pub struct TokioNetworkNode {
    handler: Arc<Mutex<UntypedHandlerBox>>,
    socket: UdpSocket,
}

impl TokioNetworkNode {
    pub fn new(handler: Arc<Mutex<UntypedHandlerBox>>, socket: UdpSocket) -> AResult<Self> {
        Ok(TokioNetworkNode { handler, socket })
    }

    pub fn start<const BUFFER_SIZE: usize, MsgT, DeT>(
        self,
        deserializer: DeT,
    ) -> TokioJoinHandle<()>
    where
        MsgT: ActorMsg,
        DeT: Fn(&mut [u8]) -> Result<MsgT, Box<dyn Error>> + Send + 'static,
    {
        tokio::spawn(async move {
            let mut data = [0u8; BUFFER_SIZE];
            loop {
                let valid_bytes = self
                    .socket
                    .recv(&mut data[..])
                    .await
                    .unwrap_or_else(|e| panic!("Receive failed: {:?}", e));
                let message = deserializer(&mut data[..valid_bytes])
                    .unwrap_or_else(|e| panic!("Deserialization failed: {:?}", e));
                if let Some(ActorControl::Exit()) = self
                    .handler
                    .lock()
                    .expect("Lock failed (poisoned)")
                    .receive_untyped(Box::new(message))
                    .expect("Unsupported message")
                {
                    break;
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct TokioP2PNetwork {
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    sockets: HashMap<String, Arc<UdpSocket>>,
}

fn send_internal<const BUFFER_SIZE: usize, MsgT, SerializerT>(
    runtime: &mut Option<Arc<tokio::runtime::Runtime>>,
    sockets: &HashMap<String, Arc<tokio::net::UdpSocket>>,
    message: MsgT,
    serializer: &SerializerT,
    node: impl AsRef<str>,
) where
    MsgT: ActorMsg,
    SerializerT: Fn(MsgT, &mut [u8]) -> Result<usize, Box<dyn Error>>,
{
    println!("Sending to {}", node.as_ref());
    let mut buffer = [0u8; BUFFER_SIZE];
    let valid =
        serializer(message, &mut buffer).unwrap_or_else(|e| panic!("Unable to serialize: {:?}", e));
    let socket_handle = sockets.get(node.as_ref()).expect("Node not found").clone();
    spawn(runtime, async move {
        socket_handle
            .send(&buffer[..valid])
            .await
            .unwrap_or_else(|e| panic!("Failed to send: {:?}", e));
    });
}

impl TokioP2PNetwork {
    pub async fn new(peers: Vec<impl Into<String>>) -> Self {
        let v = peers.into_iter().map(|p| {
            let peer = p.into();
            tokio::runtime::Handle::current().spawn(async move {
                (
                    peer,
                    Arc::new(
                        UdpSocket::bind("localhost:0")
                            .await
                            .unwrap_or_else(|e| panic!("Unable to bind: {:?}", e)),
                    ),
                )
            })
        });
        let sockets = futures::future::join_all(v)
            .await
            .into_iter()
            .map(|r| r.unwrap_or_else(|e| panic!("Unable to join: {:?}", e)))
            .collect();
        TokioP2PNetwork {
            runtime: None,
            sockets,
        }
    }

    pub async fn connect(&mut self) {
        let sockets = self.sockets.clone();
        let v = sockets.into_iter().map(|(address, socket)| {
            spawn(&mut self.runtime, async move {
                socket
                    .connect(address)
                    .await
                    .unwrap_or_else(|e| panic!("Unable to connect: {:?}", e))
            })
        });
        futures::future::join_all(v)
            .await
            .into_iter()
            .for_each(|r| {
                r.unwrap_or_else(|e| panic!("Unable to join: {:?}", e));
            });
    }
}

impl P2PNetwork for TokioP2PNetwork {
    fn send<const BUFFER_SIZE: usize, MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        serializer: &SerializerT,
        node: impl AsRef<str>,
    ) where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT, &mut [u8]) -> Result<usize, Box<dyn Error>> + Sync,
    {
        send_internal::<BUFFER_SIZE, MsgT, SerializerT>(
            &mut self.runtime,
            &self.sockets,
            message,
            serializer,
            node,
        );
    }

    fn broadcast<const BUFFER_SIZE: usize, MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        serializer: &SerializerT,
    ) where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT, &mut [u8]) -> Result<usize, Box<dyn Error>> + Sync,
    {
        for node in self.sockets.keys() {
            send_internal::<BUFFER_SIZE, MsgT, SerializerT>(
                &mut self.runtime,
                &self.sockets,
                *dyn_clone::clone_box(&message),
                serializer,
                node,
            );
        }
    }
}
