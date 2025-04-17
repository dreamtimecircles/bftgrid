use std::{
    collections::HashMap,
    fmt::Debug,
    io, mem,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use bftgrid_core::{
    ActorControl, ActorMsg, ActorRef, ActorSystem, Joinable, P2PNetwork, P2PNetworkError,
    P2PNetworkResult, Task, TypedHandler, UntypedHandler,
};
use futures::future;
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender},
    task::JoinHandle as TokioJoinHandle,
};

use crate::{cleanup_complete_tasks, new_tokio_runtime, notify_close, TokioRuntime};

#[derive(Debug)]
struct TokioTask<T> {
    underlying: TokioJoinHandle<T>,
}

impl<T> Task for TokioTask<T>
where
    T: Debug + Send,
{
    fn is_finished(&self) -> bool {
        self.underlying.is_finished()
    }
}

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

impl<MsgT, HandlerT> Task for TokioActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn is_finished(&self) -> bool {
        let (closed_mutex, _) = &*self.close_cond;
        *closed_mutex.lock().unwrap()
    }
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
}

impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for TokioActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.tx.clone();
        self.actor_system.spawn_task(async move {
            if let Some(delay_duration) = delay {
                log::debug!("Delaying send by {:?}", delay_duration);
                tokio::time::sleep(delay_duration).await;
            }
            sender.send(message).ok().unwrap()
        });
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
    runtime: Arc<TokioRuntime>,
    tasks: Arc<Mutex<Vec<TokioTask<()>>>>,
}

impl TokioActorSystem {
    pub fn new(name: impl Into<String>) -> Self {
        TokioActorSystem {
            tasks: Default::default(),
            runtime: Arc::new(new_tokio_runtime(name)),
        }
    }

    fn spawn_task(&mut self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        cleanup_complete_tasks(self.tasks.lock().unwrap().as_mut()).push(TokioTask {
            underlying: self.runtime.underlying.spawn(future),
        });
    }
}

impl Joinable<()> for TokioActorSystem {
    fn join(self) {
        self.runtime.underlying.block_on(async {
            let mut tasks = vec![];
            {
                let mut locked_tasks = self.tasks.lock().unwrap();
                mem::swap(&mut *locked_tasks, &mut tasks);
                // Drop the lock before waiting for all tasks to finish, else the actor system will deadlock on spawns
            }
            log::info!(
                "Tokio actor system '{}' joining {} tasks",
                self.runtime.name,
                tasks.len()
            );
            for t in tasks {
                t.underlying.await.unwrap();
            }
        });
    }
}

impl Task for TokioActorSystem {
    fn is_finished(&self) -> bool {
        self.tasks.lock().unwrap().iter().all(|h| h.is_finished())
    }
}

impl ActorSystem for TokioActorSystem {
    type ActorRefT<MsgT, HandlerT>
        = TokioActor<MsgT, HandlerT>
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
        let actor_system_name = self.runtime.name.clone();
        self.spawn_task(async move {
            let mut current_handler = handler_rx.recv().await.unwrap();
            log::debug!("Async actor '{}' on node '{}' started in tokio actor system '{}'", actor_name, actor_node_id, actor_system_name);
            loop {
                if let Ok(new_handler) = handler_rx.try_recv() {
                    log::debug!("Async actor '{}' on node '{}' in tokio actor system '{}': new handler received", actor_name, actor_node_id, actor_system_name);
                    current_handler = new_handler;
                }
                match rx.recv().await {
                    None => {
                        log::info!("Async actor '{}' on node '{}' in tokio actor system '{}': shutting down due to message receive channel having being closed", actor_name, actor_node_id, actor_system_name);
                        rx.close();
                        handler_rx.close();
                        notify_close(close_cond2);
                        return;
                    }
                    Some(m) => {
                        if let Some(control) = current_handler.lock().unwrap().receive(m) {
                            match control {
                                ActorControl::Exit() => {
                                    log::info!("Async actor '{}' on node '{}' in tokio actor system '{}': closing requested by handler, shutting it down", actor_name, actor_node_id, actor_system_name);
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

pub struct TokioP2PNetworkServer {
    socket: Arc<UdpSocket>,
    runtime: TokioRuntime,
}

impl TokioP2PNetworkServer {
    pub fn new(name: impl Into<String>, socket: UdpSocket) -> Self {
        TokioP2PNetworkServer {
            socket: Arc::new(socket),
            runtime: new_tokio_runtime(name),
        }
    }

    pub fn start<UntypedHandlerT: UntypedHandler + 'static, MsgT, DeT>(
        &self,
        mut handler: UntypedHandlerT,
        deserializer: DeT,
        buffer_size: usize,
    ) -> io::Result<TokioJoinHandle<()>>
    where
        MsgT: ActorMsg,
        DeT: Fn(&mut [u8]) -> P2PNetworkResult<MsgT> + Send + 'static,
    {
        let addr = self.socket.local_addr()?;
        log::info!("Async actor network server '{}' starting", addr);
        let socket = self.socket.clone();
        Ok(self.runtime.underlying.spawn(async move {
            log::info!("Async actor network server '{}' started", addr);
            let mut buf = vec![0; buffer_size];
            loop {
                log::debug!("Async actor network server '{}' receiving", addr);
                let (valid_bytes, _) = match socket.recv_from(&mut buf[..]).await {
                    Ok(bs) => bs,
                    Err(e) => {
                        log::warn!(
                            "Async actor network server '{}': datagram receive failed: {}",
                            addr,
                            e
                        );
                        continue;
                    }
                };
                log::debug!(
                    "Async actor network server '{}' received datagram, deserializing",
                    addr
                );
                let message = match deserializer(&mut buf[..valid_bytes]) {
                    Ok(m) => m,
                    Err(e) => {
                        log::warn!(
                            "Async actor network server '{}': deserialization failed: {:?}",
                            addr,
                            e
                        );
                        continue;
                    }
                };
                log::debug!(
                    "Async actor network server '{}' deserialized datagra, sending to input actor",
                    addr
                );
                if let Some(ActorControl::Exit()) = match handler.receive_untyped(Box::new(message))
                {
                    Ok(m) => m,
                    Err(e) => {
                        log::warn!(
                            "Async actor network server '{}': handler receive failed: {:?}",
                            addr,
                            e
                        );
                        continue;
                    }
                } {
                    log::debug!(
                        "Async actor network server '{}': input actor exited, stopping",
                        addr
                    );
                    break;
                }
            }
        }))
    }

    pub fn stop(self) {
        log::info!(
            "Async actor netork server '{}' stopping",
            self.socket.local_addr().unwrap()
        );
    }
}

#[derive(Debug, Clone)]
pub struct TokioP2PNetworkClient {
    runtime: Arc<TokioRuntime>,
    sockets: HashMap<String, P2PNetworkResult<Arc<UdpSocket>>>,
}

impl TokioP2PNetworkClient {
    pub fn new(name: impl Into<String>, initial_peers: Vec<impl Into<String>>) -> Self {
        let runtime = Arc::new(new_tokio_runtime(name));
        let runtime_clone = runtime.clone();
        let network_name = runtime.name.clone();
        let sockets: HashMap<String, Result<Arc<UdpSocket>, P2PNetworkError>> =
            runtime.underlying.block_on(async move {
                let initial_peer_addrs: Vec<String> =
                    initial_peers.into_iter().map(|p| p.into()).collect(); // Consumed to produce result
                let initial_peer_addrs_clone = initial_peer_addrs.clone(); // Consumed by `connect`
                initial_peer_addrs
                    .into_iter()
                    .zip(
                        future::join_all(initial_peer_addrs_clone.into_iter().map(|peer_addr| {
                            let network_name = network_name.clone();
                            runtime_clone.underlying.spawn(async move {
                                log::debug!("Async actor network client '{}' started connecting to peer {}", network_name, peer_addr);
                                let socket = UdpSocket::bind("localhost:0").await?;
                                log::debug!(
                                    "Async actor network client '{}' bound UDP socket to local address for connecting to peer {}",
                                    network_name, peer_addr
                                );
                                let p_addr_clone = peer_addr.clone();
                                socket.connect(peer_addr).await.map(|_| {
                                    log::debug!("Async actor network client '{}' connected UDP socket to peer {}", network_name, p_addr_clone);
                                    socket
                                })
                            })
                        }))
                        .await
                        .into_iter()
                        .map(|rr| match rr {
                            Ok(r) => match r {
                                Ok(socket) => {
                                    P2PNetworkResult::<Arc<UdpSocket>>::Ok(Arc::new(socket))
                                }
                                Err(e) => P2PNetworkResult::<Arc<UdpSocket>>::Err(
                                    P2PNetworkError::Io(Arc::new(e)),
                                ),
                            },
                            Err(e) => P2PNetworkResult::<Arc<UdpSocket>>::Err(
                                P2PNetworkError::Join(Arc::new(e)),
                            ),
                        }),
                    )
                    .collect()
            });
        TokioP2PNetworkClient { runtime, sockets }
    }
}

impl P2PNetwork for TokioP2PNetworkClient {
    fn attempt_send<MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        serializer: &SerializerT,
        node_addr: impl Into<String>,
    ) -> P2PNetworkResult<()>
    where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync,
    {
        attempt_send_internal(&self.runtime, &self.sockets, message, serializer, node_addr)
    }
}

fn attempt_send_internal<MsgT, SerializerT>(
    runtime: &TokioRuntime,
    sockets: &HashMap<String, P2PNetworkResult<Arc<tokio::net::UdpSocket>>>,
    message: MsgT,
    serializer: &SerializerT,
    node_addr: impl Into<String>,
) -> P2PNetworkResult<()>
where
    MsgT: ActorMsg,
    SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync,
{
    let node_addr: Arc<String> = node_addr.into().into();
    let node_addr2 = node_addr.clone();
    log::debug!(
        "Async actor network client '{}' sending to {}",
        runtime.name,
        node_addr
    );
    let socket_handle = match sockets.get(node_addr.as_ref()) {
        Some(s) => (*s).clone(),
        None => P2PNetworkResult::Err(P2PNetworkError::ActorNotFound(node_addr.clone())),
    }?;
    let serialized_message = serializer(message)?;
    let runtime_name = runtime.name.clone();
    let runtime_name2 = runtime_name.clone();
    runtime.underlying.spawn(async move {
        log::debug!(
            "Async actor network client '{}' starting network send to {}",
            runtime_name,
            node_addr
        );
        match socket_handle.send(&serialized_message[..]).await {
            Ok(_) => {
                log::debug!(
                    "Async actor network client '{}' sent a message to {}",
                    runtime_name,
                    node_addr
                );
                Ok(())
            }
            Err(e) => {
                log::warn!(
                    "Async actor network client '{}' failed to send message to {}: {:?}",
                    runtime_name,
                    node_addr,
                    e
                );
                Err(P2PNetworkError::Io(Arc::new(e)))
            }
        }
    });
    log::debug!(
        "Async actor network client '{}' started sending to {}",
        runtime_name2,
        node_addr2
    );
    Ok(())
}
