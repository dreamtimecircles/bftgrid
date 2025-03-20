use std::{
    fmt::Debug,
    mem,
    sync::{
        mpsc::{self, Sender},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle as ThreadJoinHandle},
    time::Duration,
};

use async_trait::async_trait;
use bftgrid_core::{ActorControl, ActorMsg, ActorRef, ActorSystem, Joinable, TypedHandler};

use crate::notify_close;

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
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    actor_system: ThreadActorSystem,
    tx: Sender<MsgT>,
    handler_tx: Sender<Arc<Mutex<HandlerT>>>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<MsgT, HandlerT> Joinable<()> for ThreadActor<MsgT, HandlerT>
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

#[async_trait]
impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for ThreadActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
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
                        log::debug!("Delaying send by {:?}", delay_duration);
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
        log::info!("Thread actor system joining {} tasks", tasks.len());
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
    type ActorRefT<MsgT, HandlerT>
        = ThreadActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        name: impl Into<String>,
        node_id: impl Into<String>,
    ) -> ThreadActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let (handler_tx, handler_rx) = mpsc::channel::<Arc<Mutex<HandlerT>>>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        let actor_name = name.into();
        let actor_node_id = node_id.into();
        self.tasks.lock().unwrap().push(ThreadJoinable {
            underlying: thread::spawn(move || {
                let mut current_handler = handler_rx.recv().unwrap();
                log::debug!("Actor {} on node {} started", actor_name, actor_node_id);
                loop {
                    if let Ok(new_handler) = handler_rx.try_recv() {
                        log::debug!("Actor {} on node {}: new handler received", actor_name, actor_node_id);
                        current_handler = new_handler;
                    }
                    match rx.recv() {
                        Err(_) => {
                            log::info!("Actor {} on node {}: shutting down due to message receive channel having being closed", actor_name, actor_node_id);
                            notify_close(close_cond2);
                            return;
                        }
                        Ok(m) => {
                            if let Some(control) = current_handler.lock().unwrap().receive(m) {
                                match control {
                                    ActorControl::Exit() => {
                                        log::info!("Actor {} on node {}: closing requested by handler, shutting it down", actor_name, actor_node_id);
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
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
    {
        actor_ref
            .handler_tx
            .send(Arc::new(Mutex::new(handler)))
            .unwrap();
    }
}
