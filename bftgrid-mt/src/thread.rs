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

use bftgrid_core::actor::{
    ActorControl, ActorMsg, ActorRef, ActorSystem, AnActorRef, Joinable, Task, TypedHandler,
};
use tokio::runtime::Runtime;

use crate::{cleanup_complete_tasks, notify_close, AsyncRuntime};

#[derive(Debug)]
struct ThreadJoinable<T> {
    value: ThreadJoinHandle<T>,
}

impl<T> Task for ThreadJoinable<T>
where
    T: Debug + Send,
{
    fn is_finished(&self) -> bool {
        self.value.is_finished()
    }
}

impl<T> Joinable<T> for ThreadJoinable<T>
where
    T: Debug + Send,
{
    fn join(self) -> T {
        self.value.join().unwrap()
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

impl<MsgT, HandlerT> Task for ThreadActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn is_finished(&self) -> bool {
        let (closed_mutex, _) = &*self.close_cond;
        *closed_mutex.lock().unwrap()
    }
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
}

impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for ThreadActor<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.tx.clone();
        if let Some(delay_duration) = delay {
            self.actor_system.spawn_task(move || {
                log::debug!("Delaying send by {:?}", delay_duration);
                thread::sleep(delay_duration);
                checked_send(sender, message);
            });
        } else {
            // No need to spawn a thread if no delay is needed, as the sender is non-blocking
            checked_send(sender, message);
        }
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

fn checked_send<MsgT>(sender: Sender<MsgT>, message: MsgT)
where
    MsgT: ActorMsg,
{
    match sender.send(message) {
        Ok(_) => {}
        Err(e) => {
            log::warn!("Send from thread actor failed: {:?}", e);
        }
    }
}

#[derive(Clone, Debug)]
pub struct ThreadActorSystem {
    runtime: Arc<AsyncRuntime>,
    tasks: Arc<Mutex<Vec<ThreadJoinable<()>>>>,
}

impl ThreadActorSystem {
    /// Owns the passed runtime, using it only if no contextual handle is available;
    ///  if `None` is passed, it creates a runtime with multi-threaded support,
    ///  CPU-based thread pool size and all features enabled.
    pub fn new(name: impl Into<String>, tokio: Option<Runtime>) -> Self {
        ThreadActorSystem {
            runtime: Arc::new(AsyncRuntime::new(name, tokio)),
            tasks: Default::default(),
        }
    }

    fn spawn_task(&mut self, f: impl FnOnce() + Send + 'static) {
        cleanup_complete_tasks(self.tasks.lock().unwrap().as_mut()).push(ThreadJoinable {
            value: thread::spawn(f),
        });
    }
}

impl Task for ThreadActorSystem {
    fn is_finished(&self) -> bool {
        self.tasks.lock().unwrap().iter().all(|h| h.is_finished())
    }
}

impl Joinable<()> for ThreadActorSystem {
    fn join(self) {
        let mut locked_tasks = self.tasks.lock().unwrap();
        let tasks = mem::take(&mut *locked_tasks);
        // Drop the lock before waiting for all tasks to finish, else the actor system will deadlock on spawns
        drop(locked_tasks);
        log::info!(
            "Thread actor system '{}' joining {} tasks",
            self.runtime.name,
            tasks.len()
        );
        for t in tasks {
            t.join();
        }
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
        let actor_system_name = self.runtime.name.clone();
        self.spawn_task(move || {
            let mut current_handler = handler_rx.recv().unwrap();
            log::debug!("Started actor '{}' on node '{}' in thread actor system '{}'", actor_name, actor_node_id, actor_system_name);
            loop {
                if let Ok(new_handler) = handler_rx.try_recv() {
                    log::debug!("Thread actor '{}' on node '{}' in thread actor system '{}': new handler received", actor_name, actor_node_id, actor_system_name);
                    current_handler = new_handler;
                }
                match rx.recv() {
                    Err(_) => {
                        log::info!("Thread actor '{}' on node '{}' in thread actor system '{}': shutting down due to message receive channel having being closed", actor_name, actor_node_id, actor_system_name);
                        notify_close(close_cond2);
                        return;
                    }
                    Ok(m) => {
                        if let Some(control) = current_handler.lock().unwrap().receive(m) {
                            match control {
                                ActorControl::Exit() => {
                                    log::info!("Thread actor '{}' on node '{}' in thread actor system '{}': closing requested by handler, shutting it down", actor_name, actor_node_id, actor_system_name);
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

    fn spawn_async_send<MsgT, HandlerT, O>(
        &self,
        f: impl std::prelude::rust_2024::Future<Output = O> + Send + 'static,
        to_msg: impl FnOnce(O) -> MsgT + Send + 'static,
        actor_ref: AnActorRef<MsgT, HandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
    {
        self.runtime.spawn_async_send(f, to_msg, actor_ref, delay);
    }

    fn thread_blocking_send<MsgT, HandlerT, R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
        to_msg: impl FnOnce(R) -> MsgT + Send + 'static,
        actor_ref: AnActorRef<MsgT, HandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
        R: Send + 'static,
    {
        self.runtime
            .thread_blocking_send(f, to_msg, actor_ref, delay);
    }
}
