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
use bft_grid_core::{ActorRef, ActorSystem, Joinable, TypedHandler};
use tokio::{
    runtime::Runtime,
    sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender},
};

struct TokioJoinable<OutputT> {
    runtime: Arc<Runtime>,
    join_handle: tokio::task::JoinHandle<OutputT>,
}

impl<OutputT> Joinable<OutputT> for TokioJoinable<OutputT> {
    fn join(self) -> OutputT {
        self.runtime.block_on(self.join_handle).unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.join_handle.is_finished()
    }
}
pub struct TokioActor<MsgT, HandlerT>
where
    MsgT: Send,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    runtime: Arc<Runtime>,
    tx: TUnboundedSender<MsgT>,
    handler_tx: TUnboundedSender<HandlerT>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<MsgT, HandlerT> Joinable<()> for TokioActor<MsgT, HandlerT>
where
    MsgT: Send + 'static,
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
impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for TokioActor<MsgT, HandlerT>
where
    MsgT: Send + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>> {
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

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>>
    where
        Self: Sized,
    {
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

impl Clone for TokioActorSystem {
    fn clone(&self) -> Self {
        TokioActorSystem {
            runtime: self.runtime.clone(),
        }
    }
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
    type ActorRefT<MsgT, HandlerT> = TokioActor<MsgT, HandlerT>
    where
        MsgT: 'static + Send,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        _name: String,
        _node_id: String,
    ) -> TokioActor<MsgT, HandlerT>
    where
        MsgT: 'static + Send,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (handler_tx, mut handler_rx) = tmpsc::unbounded_channel::<HandlerT>();
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
                        return;
                    }
                    Some(m) => {
                        if let Some(control) = current_handler.receive(m) {
                            match control {
                                bft_grid_core::ActorControl::Exit() => {
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
            runtime: self.runtime.clone(),
            tx,
            handler_tx,
            close_cond,
        }
    }

    fn set_handler<MsgT: Send + 'static, HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static>(
        &mut self,
        actor_ref: &mut TokioActor<MsgT, HandlerT>,
        handler: HandlerT,
    ) {
        actor_ref.handler_tx.send(handler).unwrap();
    }
}

struct ThreadJoinable<OutputT> {
    join_handle: thread::JoinHandle<OutputT>,
}

impl<OutputT> Joinable<OutputT> for ThreadJoinable<OutputT> {
    fn join(self) -> OutputT {
        self.join_handle.join().unwrap()
    }

    fn is_finished(&mut self) -> bool {
        self.join_handle.is_finished()
    }
}

pub struct ThreadActor<MsgT, HandlerT>
where
    MsgT: Send,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    tx: Sender<MsgT>,
    handler_tx: Sender<HandlerT>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<MsgT, HandlerT> Joinable<()> for ThreadActor<MsgT, HandlerT>
where
    MsgT: Send,
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
    MsgT: Send + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>> {
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

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>>
    where
        Self: Sized,
    {
        Box::new(ThreadActor {
            tx: self.tx.clone(),
            handler_tx: self.handler_tx.clone(),
            close_cond: self.close_cond.clone(),
        })
    }
}

pub struct ThreadActorSystem {}

impl Clone for ThreadActorSystem {
    fn clone(&self) -> Self {
        ThreadActorSystem {}
    }
}

impl ActorSystem for ThreadActorSystem {
    type ActorRefT<MsgT, HandlerT> = ThreadActor<MsgT, HandlerT>
    where
        MsgT: 'static + Send,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        _name: String,
        _node_id: String,
    ) -> ThreadActor<MsgT, HandlerT>
    where
        MsgT: 'static + Send,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let (handler_tx, handler_rx) = mpsc::channel::<HandlerT>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        thread::spawn(move || {
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
                        if let Some(control) = current_handler.receive(m) {
                            match control {
                                bft_grid_core::ActorControl::Exit() => {
                                    notify_close(close_cond2);
                                    return;
                                }
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

    fn set_handler<MsgT, HandlerT>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: 'static + Send,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
    {
        actor_ref.handler_tx.send(handler).unwrap();
    }
}
