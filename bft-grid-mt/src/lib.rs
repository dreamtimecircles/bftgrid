use std::{
    sync::mpsc,
    sync::{
        mpsc::{RecvError, Sender},
        Arc, Condvar, Mutex,
    },
    thread::{self},
    time::Duration,
};

use async_trait::async_trait;
use bft_grid_core::{ActorRef, ActorSystem, Joinable, SingleThreadedActorRef, TypedMessageHandler};
use tokio::{
    runtime::Runtime,
    sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender},
};

struct TokioJoinable<Output> {
    runtime: Arc<Runtime>,
    join_handle: tokio::task::JoinHandle<Output>,
}

impl<Output> Joinable<Output> for TokioJoinable<Output> {
    fn join(self) -> Output {
        self.runtime.block_on(self.join_handle).unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.join_handle.is_finished()
    }
}
pub struct TokioActor<Msg, MessageHandler>
where
    Msg: Send,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
{
    runtime: Arc<Runtime>,
    tx: TUnboundedSender<Msg>,
    handler_tx: TUnboundedSender<MessageHandler>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<Msg, MessageHandler> Joinable<()> for TokioActor<Msg, MessageHandler>
where
    Msg: Send + 'static,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
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
impl<Msg, MessageHandler> SingleThreadedActorRef<Msg> for TokioActor<Msg, MessageHandler>
where
    Msg: Send + 'static,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
{
    fn send(&mut self, message: Msg, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>> {
        let sender = self.tx.clone();
        Box::new(TokioJoinable {
            runtime: self.runtime.clone(),
            join_handle: self.runtime.spawn(async move {
                if let Some(delay_duration) = delay {
                    tokio::time::sleep(delay_duration).await;
                }
                sender.send(message).ok()
            }),
        })
    }
}

impl<Msg, MessageHandler> ActorRef<Msg, MessageHandler> for TokioActor<Msg, MessageHandler>
where
    Msg: Send + 'static,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + Send + 'static,
{
    fn clone(&self) -> Box<dyn ActorRef<Msg, MessageHandler>> {
        Box::new(TokioActor {
            runtime: self.runtime.clone(),
            tx: self.tx.clone(),
            handler_tx: self.handler_tx.clone(),
            close_cond: self.close_cond.clone(),
        })
    }
}

pub struct TokioActorSystem {
    runtime: Arc<Runtime>,
}

impl TokioActorSystem {
    pub fn new() -> Self {
        TokioActorSystem {
            runtime: Arc::new(Runtime::new().unwrap()),
        }
    }
}

impl Default for TokioActorSystem {
    fn default() -> Self {
        TokioActorSystem::new()
    }
}

fn notify_close(close_cond: Arc<(Mutex<bool>, Condvar)>) {
    let (closed_mutex, cvar) = &*close_cond;
    let mut closed = closed_mutex.lock().unwrap();
    *closed = true;
    cvar.notify_all();
}

impl ActorSystem for TokioActorSystem {
    type ConcreteActorRef<
        Msg: Send + 'static,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
    > = TokioActor<Msg, MessageHandler>;

    fn create<Msg, MessageHandler>(
        &mut self,
        _node_name: String,
        _actor_name: String,
    ) -> TokioActor<Msg, MessageHandler>
    where
        Msg: 'static + Send,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + Send + 'static,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (handler_tx, mut handler_rx) = tmpsc::unbounded_channel::<MessageHandler>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        self.runtime.spawn(async move {
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
                        break;
                    }
                    Some(m) => {
                        if let Some(control) = current_handler.receive(m) {
                            match control {
                                bft_grid_core::ActorControl::Exit() => {
                                    rx.close();
                                    handler_rx.close();
                                    notify_close(close_cond2);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        TokioActor {
            runtime: self.runtime.clone(),
            tx,
            handler_tx,
            close_cond,
        }
    }

    fn set_handler<
        Msg: Send + 'static,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
    >(
        &mut self,
        actor_ref: &mut TokioActor<Msg, MessageHandler>,
        handler: MessageHandler,
    ) {
        actor_ref.handler_tx.send(handler).unwrap();
    }
}

struct ThreadJoinable<Output> {
    join_handle: thread::JoinHandle<Output>,
}

impl<Output> Joinable<Output> for ThreadJoinable<Output> {
    fn join(self) -> Output {
        self.join_handle.join().unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.join_handle.is_finished()
    }
}

pub struct ThreadActor<Msg, MessageHandler>
where
    Msg: Send,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
{
    tx: Sender<Msg>,
    handler_tx: Sender<MessageHandler>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<Msg, MessageHandler> Joinable<()> for ThreadActor<Msg, MessageHandler>
where
    Msg: Send,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
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
impl<Msg, MessageHandler> SingleThreadedActorRef<Msg> for ThreadActor<Msg, MessageHandler>
where
    Msg: Send + 'static,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
{
    fn send(&mut self, message: Msg, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>> {
        let sender = self.tx.clone();
        Box::new(ThreadJoinable {
            join_handle: thread::spawn(move || {
                if let Some(delay_duration) = delay {
                    thread::sleep(delay_duration);
                }
                sender.send(message).ok()
            }),
        })
    }
}

impl<Msg, MessageHandler> ActorRef<Msg, MessageHandler> for ThreadActor<Msg, MessageHandler>
where
    Msg: Send + 'static,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + Send + 'static,
{
    fn clone(&self) -> Box<dyn ActorRef<Msg, MessageHandler>> {
        Box::new(ThreadActor {
            tx: self.tx.clone(),
            handler_tx: self.handler_tx.clone(),
            close_cond: self.close_cond.clone(),
        })
    }
}

pub struct ThreadActorSystem {}

impl ActorSystem for ThreadActorSystem {
    type ConcreteActorRef<Msg, MessageHandler> = ThreadActor<Msg, MessageHandler>
    where
        Msg: 'static + Send,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static;

    fn create<Msg, MessageHandler>(
        &mut self,
        _node_name: String,
        _actor_name: String,
    ) -> ThreadActor<Msg, MessageHandler>
    where
        Msg: 'static + Send,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let (handler_tx, handler_rx) = mpsc::channel::<MessageHandler>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        thread::spawn(move || loop {
            let mut current_handler = handler_rx.recv().unwrap();
            if let Ok(new_handler) = handler_rx.try_recv() {
                current_handler = new_handler;
            }
            match rx.recv() {
                Err(RecvError) => {
                    notify_close(close_cond2);
                    break;
                }
                Ok(m) => {
                    if let Some(control) = current_handler.receive(m) {
                        match control {
                            bft_grid_core::ActorControl::Exit() => {
                                notify_close(close_cond2);
                                break;
                            }
                        }
                    }
                }
            }
        });
        ThreadActor {
            tx,
            handler_tx,
            close_cond,
        }
    }

    fn set_handler<Msg, MessageHandler>(
        &mut self,
        actor_ref: &mut Self::ConcreteActorRef<Msg, MessageHandler>,
        handler: MessageHandler,
    ) where
        Msg: 'static + Send,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
    {
        actor_ref.handler_tx.send(handler).unwrap();
    }
}
