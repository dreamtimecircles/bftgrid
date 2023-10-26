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
    fn join(self: Box<Self>) -> Output {
        self.runtime.block_on(self.join_handle).unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.join_handle.is_finished()
    }
}
struct TokioActor<M>
where
    M: Send,
{
    runtime: Arc<Runtime>,
    tx: TUnboundedSender<M>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<Msg> Joinable<()> for TokioActor<Msg>
where
    Msg: Send + 'static,
{
    fn join(self: Box<Self>) {
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
impl<Msg> SingleThreadedActorRef<Msg> for TokioActor<Msg>
where
    Msg: Send + 'static,
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

impl<Msg> ActorRef<Msg> for TokioActor<Msg>
where
    Msg: Send + 'static,
{
    fn clone(&self) -> Box<dyn ActorRef<Msg>> {
        Box::new(TokioActor {
            runtime: self.runtime.clone(),
            tx: self.tx.clone(),
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
    fn spawn_actor<Msg, MessageHandler>(
        &mut self,
        _node_name: String,
        _actor_name: String,
        mut handler: MessageHandler,
    ) -> Box<dyn ActorRef<Msg>>
    where
        Msg: 'static + Send,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        self.runtime.spawn(async move {
            loop {
                match rx.recv().await {
                    None => {
                        rx.close();
                        notify_close(close_cond2);
                        break;
                    }
                    Some(m) => {
                        if let Some(control) = handler.receive(m) {
                            match control {
                                bft_grid_core::ActorControl::Exit() => {
                                    rx.close();
                                    notify_close(close_cond2);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        Box::new(TokioActor {
            runtime: self.runtime.clone(),
            tx,
            close_cond,
        })
    }
}

struct ThreadJoinable<Output> {
    join_handle: thread::JoinHandle<Output>,
}

impl<Output> Joinable<Output> for ThreadJoinable<Output> {
    fn join(self: Box<Self>) -> Output {
        self.join_handle.join().unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.join_handle.is_finished()
    }
}

struct ThreadActor<Msg>
where
    Msg: Send,
{
    tx: Sender<Msg>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<Msg> Joinable<()> for ThreadActor<Msg>
where
    Msg: Send,
{
    fn join(self: Box<Self>) {
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
impl<Msg> SingleThreadedActorRef<Msg> for ThreadActor<Msg>
where
    Msg: Send + 'static,
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

impl<Msg> ActorRef<Msg> for ThreadActor<Msg>
where
    Msg: Send + 'static,
{
    fn clone(&self) -> Box<dyn ActorRef<Msg>> {
        todo!()
    }
}

pub struct ThreadActorSystem {}

impl ActorSystem for ThreadActorSystem {
    fn spawn_actor<Msg, MessageHandler: 'static>(
        &mut self,
        _node_name: String,
        _actor_name: String,
        mut handler: MessageHandler,
    ) -> Box<dyn ActorRef<Msg>>
    where
        Msg: 'static + Send,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    {
        let (tx, rx) = mpsc::channel();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        thread::spawn(move || loop {
            match rx.recv() {
                Err(RecvError) => {
                    notify_close(close_cond2);
                    break;
                }
                Ok(m) => {
                    if let Some(control) = handler.receive(m) {
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
        Box::new(ThreadActor { tx, close_cond })
    }
}
