use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    io, mem,
    sync::{Arc, Condvar},
    time::Duration,
};

use bftgrid_core::actor::{
    ActorControl, ActorMsg, ActorRef, ActorSystemHandle, AnActorRef, Joinable, P2PNetworkClient,
    P2PNetworkError, P2PNetworkResult, Task, TypedMsgHandler, UntypedMsgHandler,
};
use futures::future;
use tokio::{
    net::UdpSocket,
    runtime::Runtime,
    sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender},
    task::JoinHandle as TokioJoinHandle,
};

use crate::{
    join_tasks, notify_close, push_async_task, spawn_async_task, AsyncRuntime, ThreadJoinable,
    TokioTask,
};

#[derive(Debug)]
struct TokioActorData<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    tx: TUnboundedSender<MsgT>,
    handler_tx: TUnboundedSender<Arc<tokio::sync::Mutex<MsgHandlerT>>>,
    close_cond: Arc<(std::sync::Mutex<bool>, Condvar)>,
}

impl<MsgT, MsgHandlerT> Clone for TokioActorData<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn clone(&self) -> Self {
        TokioActorData {
            tx: self.tx.clone(),
            handler_tx: self.handler_tx.clone(),
            close_cond: self.close_cond.clone(),
        }
    }
}

#[derive(Debug)]
pub struct TokioActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    data: TokioActorData<MsgT, MsgHandlerT>,
    actor_system: TokioActorSystemHandle,
}

impl<MsgT, MsgHandlerT> Task for TokioActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn is_finished(&self) -> bool {
        let (closed_mutex, _) = &*self.data.close_cond;
        *closed_mutex.lock().unwrap()
    }
}

impl<MsgT, MsgHandlerT> Joinable<()> for TokioActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn join(&mut self) {
        let (close_mutex, cvar) = &*self.data.close_cond;
        let mut closed = close_mutex.lock().unwrap(); // Shortly held lock, no need to block the thread via the runtime
        while !*closed {
            // The wait can be long, so block the tread safely via the runtime.
            //  In any case, since the actor system is being dropped and this
            //  is a one-time operation, it does not constitute a performance issue.
            let runtime = self
                .actor_system
                .actor_system
                .lock()
                .unwrap()
                .runtime
                .clone();
            closed = runtime.thread_blocking(|| cvar.wait(closed).unwrap());
        }
    }
}

impl<MsgT, MsgHandlerT> ActorRef<MsgT, MsgHandlerT> for TokioActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.data.tx.clone();
        let mut actor_system = self.actor_system.actor_system.lock().unwrap();
        actor_system.spawn_async_task(async move {
            if let Some(delay_duration) = delay {
                log::debug!("Delaying send by {:?}", delay_duration);
                tokio::time::sleep(delay_duration).await;
            }
            checked_send(sender, message);
        });
    }

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, MsgHandlerT>> {
        Box::new(TokioActor {
            data: self.data.clone(),
            actor_system: self.actor_system.clone(),
        })
    }
}

fn checked_send<MsgT>(sender: TUnboundedSender<MsgT>, message: MsgT)
where
    MsgT: ActorMsg,
{
    match sender.send(message) {
        Ok(_) => {}
        Err(e) => {
            log::warn!("Send from tokio actor failed: {:?}", e);
        }
    }
}

#[derive(Debug)]
pub struct TokioActorSystem {
    runtime: Arc<AsyncRuntime>,
    async_tasks: Vec<TokioTask<()>>,
    join_tasks_on_drop: bool,
}

impl TokioActorSystem {
    fn spawn_async_task(&mut self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        spawn_async_task(&mut self.async_tasks, &self.runtime, future);
    }

    fn extract_tasks(&mut self) -> (Vec<ThreadJoinable<()>>, Vec<TokioTask<()>>) {
        (vec![], mem::take(&mut self.async_tasks))
    }

    fn create<MsgT, MsgHandlerT>(
        &mut self,
        name: impl Into<String>,
        node_id: impl Into<String>,
    ) -> TokioActorData<MsgT, MsgHandlerT>
    where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (handler_tx, mut handler_rx) =
            tmpsc::unbounded_channel::<Arc<tokio::sync::Mutex<MsgHandlerT>>>();
        let close_cond = Arc::new((std::sync::Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        let actor_name = name.into();
        let actor_node_id = node_id.into();
        let actor_system_name = self.runtime.name.clone();
        self.spawn_async_task(async move {
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
                        if let Some(control) = current_handler.lock().await.receive(m) {
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
        TokioActorData {
            tx,
            handler_tx,
            close_cond,
        }
    }

    fn set_handler<MsgT, MsgHandlerT>(
        &mut self,
        actor_ref: &mut TokioActor<MsgT, MsgHandlerT>,
        handler: MsgHandlerT,
    ) where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        actor_ref
            .data
            .handler_tx
            .send(Arc::new(tokio::sync::Mutex::new(handler)))
            .unwrap();
    }

    fn spawn_async_send<MsgT, MsgHandlerT>(
        &mut self,
        f: impl Future<Output = MsgT> + Send + 'static,
        actor_ref: AnActorRef<MsgT, MsgHandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        push_async_task(
            &mut self.async_tasks,
            self.runtime.spawn_async_send(f, actor_ref, delay),
        );
    }

    fn spawn_thread_blocking_send<MsgT, MsgHandlerT>(
        &mut self,
        f: impl FnOnce() -> MsgT + Send + 'static,
        actor_ref: AnActorRef<MsgT, MsgHandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        push_async_task(
            &mut self.async_tasks,
            self.runtime.spawn_thread_blocking_send(f, actor_ref, delay),
        );
    }
}

impl Task for TokioActorSystem {
    fn is_finished(&self) -> bool {
        self.async_tasks.iter().all(|h| h.is_finished())
    }
}

impl Drop for TokioActorSystem {
    fn drop(&mut self) {
        if self.join_tasks_on_drop {
            log::debug!(
                "Tokio actor system '{}' dropping, joining tasks",
                self.runtime.name
            );
            join_tasks(self.runtime.clone().as_ref(), self.extract_tasks());
        } else {
            log::debug!(
                "Tokio actor system '{}' dropping, not joining tasks",
                self.runtime.name
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct TokioActorSystemHandle {
    actor_system: Arc<std::sync::Mutex<TokioActorSystem>>,
}

impl TokioActorSystemHandle {
    /// Owns the passed runtime, using it only if no contextual handle is available;
    ///  if `None` is passed, it creates a runtime with multi-threaded support,
    ///  CPU-based thread pool size and all features enabled.
    pub fn new_actor_system(
        name: impl Into<String>,
        tokio: Option<Runtime>,
        join_tasks_on_drop: bool,
    ) -> Self {
        let runtime = AsyncRuntime::new(name, tokio);
        let actor_system = TokioActorSystem {
            runtime: Arc::new(runtime),
            async_tasks: vec![],
            join_tasks_on_drop,
        };
        TokioActorSystemHandle {
            actor_system: Arc::new(std::sync::Mutex::new(actor_system)),
        }
    }
}

impl ActorSystemHandle for TokioActorSystemHandle {
    type ActorRefT<MsgT, MsgHandlerT>
        = TokioActor<MsgT, MsgHandlerT>
    where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static;

    fn create<MsgT, MsgHandlerT>(
        &self,
        node_id: impl Into<String>,
        name: impl Into<String>,
    ) -> Self::ActorRefT<MsgT, MsgHandlerT>
    where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT>,
    {
        TokioActor {
            data: self.actor_system.lock().unwrap().create(name, node_id),
            actor_system: self.clone(),
        }
    }

    fn set_handler<MsgT, MsgHandlerT>(
        &self,
        actor_ref: &mut Self::ActorRefT<MsgT, MsgHandlerT>,
        handler: MsgHandlerT,
    ) where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT>,
    {
        self.actor_system
            .lock()
            .unwrap()
            .set_handler(actor_ref, handler);
    }

    fn spawn_async_send<MsgT, MsgHandlerT>(
        &self,
        f: impl Future<Output = MsgT> + Send + 'static,
        actor_ref: AnActorRef<MsgT, MsgHandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        self.actor_system
            .lock()
            .unwrap()
            .spawn_async_send(f, actor_ref, delay);
    }

    fn spawn_thread_blocking_send<MsgT, MsgHandlerT>(
        &self,
        f: impl FnOnce() -> MsgT + Send + 'static,
        actor_ref: AnActorRef<MsgT, MsgHandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        self.actor_system
            .lock()
            .unwrap()
            .spawn_thread_blocking_send(f, actor_ref, delay);
    }
}

impl Task for TokioActorSystemHandle {
    fn is_finished(&self) -> bool {
        self.actor_system.lock().unwrap().is_finished()
    }
}

impl Joinable<()> for TokioActorSystemHandle {
    fn join(&mut self) {
        let mut actor_system_lock_guard = self.actor_system.lock().unwrap();
        let async_runtime = actor_system_lock_guard.runtime.clone();
        let tasks = actor_system_lock_guard.extract_tasks();
        // Drop the lock before joining tasks to avoid deadlocks if they also lock the actor system
        drop(actor_system_lock_guard);
        join_tasks(async_runtime.as_ref(), tasks);
    }
}

pub struct TokioP2PNetworkServer {
    socket: Arc<UdpSocket>,
    runtime: AsyncRuntime,
}

impl TokioP2PNetworkServer {
    /// Owns the passed runtime, using it only if no contextual handle is available;
    ///  if `None` is passed, it creates a runtime with multi-threaded support,
    ///  CPU-based thread pool size and all features enabled.
    pub fn new(name: impl Into<String>, socket: UdpSocket, tokio: Option<Runtime>) -> Self {
        TokioP2PNetworkServer {
            socket: Arc::new(socket),
            runtime: AsyncRuntime::new(name, tokio),
        }
    }

    pub fn start<UntypedMsgHandlerT: UntypedMsgHandler + 'static, MsgT, DeT>(
        &self,
        mut handler: UntypedMsgHandlerT,
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
        Ok(self.runtime.spawn_async(async move {
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
}

#[derive(Debug, Clone)]
pub struct TokioP2PNetworkClient {
    runtime: Arc<AsyncRuntime>,
    sockets: HashMap<String, P2PNetworkResult<Arc<UdpSocket>>>,
}

impl TokioP2PNetworkClient {
    /// Owns the passed runtime, using it only if no contextual handle is available;
    ///  if `None` is passed, it creates a runtime with multi-threaded support,
    ///  CPU-based thread pool size and all features enabled.
    pub fn new(
        name: impl Into<String>,
        initial_peers: Vec<impl Into<String>>,
        tokio: Option<Runtime>,
    ) -> Self {
        let runtime = Arc::new(AsyncRuntime::new(name, tokio));
        let runtime_clone = runtime.clone();
        let network_name = runtime.name.clone();
        // `block_on_async` is used here to bind Tokio UDP sockets to peer addresses from any
        //  context, async or thread-blocking. This is a one-time operation and is fast.
        let sockets: HashMap<String, Result<Arc<UdpSocket>, P2PNetworkError>> =
            runtime.block_on_async(async {
                let initial_peer_addrs: Vec<String> =
                    initial_peers.into_iter().map(|p| p.into()).collect(); // Consumed to produce result
                let initial_peer_addrs_clone = initial_peer_addrs.clone(); // Consumed by `connect`
                initial_peer_addrs
                    .into_iter()
                    .zip(
                        future::join_all(initial_peer_addrs_clone.into_iter().map(|peer_addr| {
                            let network_name = network_name.clone();
                            runtime_clone.spawn_async(async move {
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

impl P2PNetworkClient for TokioP2PNetworkClient {
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
    runtime: &AsyncRuntime,
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
    // `block_on_async` is used send over async UDP sockets from any context and is expected to be fast.
    runtime.block_on_async(async {
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
    })
}
