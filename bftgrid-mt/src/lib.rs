use std::{
    fmt::Debug,
    mem,
    sync::{
        mpsc::{self, RecvError, Sender},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle as ThreadJoinHandle},
    time::Duration,
};

use async_trait::async_trait;
use bftgrid_core::{
    ActorControl, ActorMsg, ActorRef, ActorSystem, Joinable, TypedHandler, UntypedHandlerBox,
};
use serde::{Deserialize, Deserializer, Serialize};
use tokio::{
    net::UdpSocket,
    runtime::Runtime,
    sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender},
    task::JoinHandle as TokioJoinHandle,
};

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

#[derive(Debug)]
struct TokioJoinable<T> {
    runtime: Arc<Runtime>,
    underlying: TokioJoinHandle<T>,
}

impl<T> Joinable<T> for TokioJoinable<T>
where
    T: Debug + Send,
{
    fn join(self) -> T {
        self.runtime.block_on(self.underlying).unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.underlying.is_finished()
    }
}

#[async_trait]
impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for TokioActor<MsgT, HandlerT>
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.tx.clone();
        self.actor_system.tasks.lock().unwrap().push(TokioJoinable {
            runtime: self.actor_system.runtime.clone(),
            underlying: self.actor_system.runtime.spawn(async move {
                if let Some(delay_duration) = delay {
                    tokio::time::sleep(delay_duration).await;
                }
                sender.send(message).ok().unwrap()
            }),
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
    runtime: Arc<Runtime>,
    tasks: Arc<Mutex<Vec<TokioJoinable<()>>>>,
}

impl TokioActorSystem {
    pub fn new() -> Self {
        TokioActorSystem {
            runtime: Arc::new(Runtime::new().unwrap()),
            tasks: Default::default(),
        }
    }
}

impl Default for TokioActorSystem {
    fn default() -> Self {
        TokioActorSystem::new()
    }
}

impl Joinable<()> for TokioActorSystem {
    fn join(self) {
        let mut locked_tasks = self.tasks.lock().unwrap();
        let mut tasks = vec![];
        mem::swap(&mut *locked_tasks, &mut tasks);
        drop(locked_tasks); // Drop the lock before waiting for all tasks to finish, else the actor system will deadlock on spawns
        for t in tasks {
            t.join();
        }
    }

    fn is_finished(&mut self) -> bool {
        self.tasks
            .lock()
            .unwrap()
            .iter_mut()
            .all(|h| h.is_finished())
    }
}

fn notify_close(close_cond: Arc<(Mutex<bool>, Condvar)>) {
    let (closed_mutex, cvar) = &*close_cond;
    let mut closed = closed_mutex.lock().unwrap();
    *closed = true;
    cvar.notify_all();
}

impl ActorSystem for TokioActorSystem {
    type ActorRefT<MsgT, HandlerT> = TokioActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        _name: String,
        _node_id: String,
    ) -> TokioActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (handler_tx, mut handler_rx) = tmpsc::unbounded_channel::<Arc<Mutex<HandlerT>>>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        self.tasks.lock().unwrap().push(TokioJoinable {
            runtime: self.runtime.clone(),
            underlying: self.runtime.spawn(async move {
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
            }),
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
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    {
        actor_ref
            .handler_tx
            .send(Arc::new(Mutex::new(handler)))
            .unwrap();
    }
}

#[derive(Debug)]
struct ThreadJoinable<T> {
    underlying: ThreadJoinHandle<T>,
}

impl<T> Joinable<T> for ThreadJoinable<T>
where
    T: Debug + Send,
{
    fn join(self) -> T {
        self.underlying.join().unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.underlying.is_finished()
    }
}

#[derive(Debug)]
pub struct ThreadActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    actor_system: ThreadActorSystem,
    tx: Sender<MsgT>,
    handler_tx: Sender<Arc<Mutex<HandlerT>>>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<MsgT, HandlerT> Joinable<()> for ThreadActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
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

#[async_trait]
impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for ThreadActor<MsgT, HandlerT>
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.tx.clone();
        self.actor_system
            .tasks
            .lock()
            .unwrap()
            .push(ThreadJoinable {
                underlying: thread::spawn(move || {
                    if let Some(delay_duration) = delay {
                        thread::sleep(delay_duration);
                    }
                    sender.send(message).ok().unwrap()
                }),
            });
    }

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>> {
        Box::new(ThreadActor {
            actor_system: self.actor_system.clone(),
            tx: self.tx.clone(),
            handler_tx: self.handler_tx.clone(),
            close_cond: self.close_cond.clone(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct ThreadActorSystem {
    tasks: Arc<Mutex<Vec<ThreadJoinable<()>>>>,
}

impl ThreadActorSystem {
    pub fn new() -> Self {
        ThreadActorSystem {
            tasks: Default::default(),
        }
    }
}

impl Default for ThreadActorSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl Joinable<()> for ThreadActorSystem {
    fn join(self) {
        let mut locked_tasks = self.tasks.lock().unwrap();
        let mut tasks = vec![];
        mem::swap(&mut *locked_tasks, &mut tasks);
        drop(locked_tasks); // Drop the lock before waiting for all tasks to finish, else the actor system will deadlock on spawns
        for t in tasks {
            t.join();
        }
    }

    fn is_finished(&mut self) -> bool {
        self.tasks
            .lock()
            .unwrap()
            .iter_mut()
            .all(|h| h.is_finished())
    }
}

impl ActorSystem for ThreadActorSystem {
    type ActorRefT<MsgT, HandlerT> = ThreadActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        _name: String,
        _node_id: String,
    ) -> ThreadActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let (handler_tx, handler_rx) = mpsc::channel::<Arc<Mutex<HandlerT>>>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        self.tasks.lock().unwrap().push(ThreadJoinable {
            underlying: thread::spawn(move || {
                let mut current_handler = handler_rx.recv().unwrap();
                loop {
                    if let Ok(new_handler) = handler_rx.try_recv() {
                        current_handler = new_handler;
                    }
                    match rx.recv() {
                        Err(RecvError) => {
                            notify_close(close_cond2);
                            return;
                        }
                        Ok(m) => {
                            if let Some(control) = current_handler.lock().unwrap().receive(m) {
                                match control {
                                    ActorControl::Exit() => {
                                        notify_close(close_cond2);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }),
        });
        ThreadActor {
            actor_system: self.clone(),
            tx,
            handler_tx,
            close_cond,
        }
    }

    fn set_handler<MsgT, HandlerT>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, HandlerT>,
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
    pub async fn new(
        handler: Arc<Mutex<UntypedHandlerBox>>,
        socket: UdpSocket,
    ) -> Result<Self, ()> {
        Ok(TokioNetworkNode { handler, socket })
    }

    pub fn start<'de, MsgT, DeT, DeCT, const BUFFER_SIZE: usize>(
        self,
        new_deserializer: DeCT,
    ) -> TokioJoinHandle<()>
    where
        MsgT: ActorMsg + Serialize + Deserialize<'de>,
        DeT: Deserializer<'de>,
        DeCT: Fn(&mut [u8]) -> DeT + Send + 'static,
    {
        tokio::spawn(async move {
            let mut data = [0u8; BUFFER_SIZE];
            loop {
                let valid_bytes = self
                    .socket
                    .recv(&mut data[..])
                    .await
                    .unwrap_or_else(|e| panic!("Receive failed: {:?}", e));
                let message = MsgT::deserialize(new_deserializer(&mut data[..valid_bytes]))
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
