use std::{
    fmt::Debug,
    future::Future,
    mem,
    sync::{
        mpsc::{self, Sender},
        Arc, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

use crate::{
    cleanup_complete_tasks, join_tasks, notify_close, push_async_task, AsyncRuntime,
    ThreadJoinable, TokioTask,
};
use bftgrid_core::actor::{
    ActorControl, ActorMsg, ActorRef, ActorSystemHandle, DynMsgHandler, Joinable, Task,
};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle as TokioJoinHandle;

#[derive(Debug)]
pub struct ThreadActorData<MsgT>
where
    MsgT: ActorMsg,
{
    tx: Sender<MsgT>,
    handler_tx: Sender<Arc<Mutex<DynMsgHandler<MsgT>>>>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
    name: Arc<String>,
}

#[derive(Debug)]
struct ThreadActor<MsgT>
where
    MsgT: ActorMsg,
{
    data: ThreadActorData<MsgT>,
    actor_system_handle: ThreadActorSystemHandle,
    join_on_drop: bool,
}

impl<Msg> ThreadActor<Msg>
where
    Msg: ActorMsg,
{
    fn join(&self) {
        let (close_mutex, cvar) = &*self.data.close_cond;
        let mut closed = close_mutex.lock().unwrap();
        while !*closed {
            closed = cvar.wait(closed).unwrap();
        }
    }
}

impl<MsgT> Drop for ThreadActor<MsgT>
where
    MsgT: ActorMsg,
{
    fn drop(&mut self) {
        if self.join_on_drop {
            log::debug!("Thread actor '{}' dropping, joining", self.data.name);
            self.join();
        } else {
            log::debug!("Thread actor '{}' dropping, not joining", self.data.name);
        }
    }
}

impl<MsgT> Task for ThreadActor<MsgT>
where
    MsgT: ActorMsg,
{
    fn is_finished(&self) -> bool {
        *self.data.close_cond.0.lock().unwrap()
    }
}

#[derive(Debug)]
pub struct ThreadActorRef<MsgT>
where
    MsgT: ActorMsg,
{
    actor: Arc<ThreadActor<MsgT>>,
}

impl<MsgT> Clone for ThreadActorRef<MsgT>
where
    MsgT: ActorMsg,
{
    fn clone(&self) -> Self {
        ThreadActorRef {
            actor: self.actor.clone(),
        }
    }
}

impl<MsgT> Task for ThreadActorRef<MsgT>
where
    MsgT: ActorMsg,
{
    fn is_finished(&self) -> bool {
        self.actor.is_finished()
    }
}

impl<MsgT> Joinable<()> for ThreadActorRef<MsgT>
where
    MsgT: ActorMsg,
{
    fn join(&mut self) {
        self.actor.join();
    }
}

impl<MsgT> ActorRef<MsgT> for ThreadActorRef<MsgT>
where
    MsgT: ActorMsg,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.actor.data.tx.clone();
        if let Some(delay_duration) = delay {
            self.actor
                .actor_system_handle
                .spawn_thread_blocking_task(move || {
                    log::debug!("Delaying send by {:?}", delay_duration);
                    thread::sleep(delay_duration);
                    checked_send(sender, message);
                });
        } else {
            // No need to spawn a thread if no delay is needed, as the sender is non-blocking
            checked_send(sender, message);
        }
    }

    fn set_handler(&mut self, handler: DynMsgHandler<MsgT>) {
        self.actor
            .data
            .handler_tx
            .send(Arc::new(Mutex::new(handler)))
            .unwrap();
    }

    fn spawn_async_send(
        &mut self,
        f: impl Future<Output = MsgT> + Send + 'static,
        delay: Option<Duration>,
    ) {
        let mut self_clone = self.clone();
        let mut actor_system_lock_guard =
            self.actor.actor_system_handle.actor_system.lock().unwrap();
        let async_runtime = actor_system_lock_guard.async_runtime.clone();
        actor_system_lock_guard.push_async_task(async_runtime.spawn_async(async move {
            self_clone.send(f.await, delay);
        }));
    }

    fn spawn_thread_blocking_send(
        &mut self,
        f: impl FnOnce() -> MsgT + Send + 'static,
        delay: Option<Duration>,
    ) {
        let self_clone = self.clone();
        let mut actor_system_lock_guard =
            self.actor.actor_system_handle.actor_system.lock().unwrap();
        let async_runtime = actor_system_lock_guard.async_runtime.clone();
        actor_system_lock_guard
            .push_async_task(async_runtime.spawn_thread_blocking_send(f, self_clone, delay));
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

#[derive(Debug)]
pub struct ThreadActorSystem {
    async_runtime: Arc<AsyncRuntime>,
    thread_blocking_tasks: Vec<ThreadJoinable<()>>,
    async_tasks: Vec<TokioTask<()>>,
    join_tasks_on_drop: bool,
}

impl ThreadActorSystem {
    fn spawn_thread_blocking_task(&mut self, f: impl FnOnce() + Send + 'static) {
        cleanup_complete_tasks(&mut self.thread_blocking_tasks).push(ThreadJoinable {
            value: Some(thread::spawn(f)),
        });
    }

    fn extract_tasks(&mut self) -> (Vec<ThreadJoinable<()>>, Vec<TokioTask<()>>) {
        (
            mem::take(&mut self.thread_blocking_tasks),
            mem::take(&mut self.async_tasks),
        )
    }

    fn create<MsgT>(
        &mut self,
        name: impl Into<String>,
        node_id: impl Into<String>,
    ) -> ThreadActorData<MsgT>
    where
        MsgT: ActorMsg,
    {
        let (tx, rx) = mpsc::channel();
        let (handler_tx, handler_rx) = mpsc::channel::<Arc<Mutex<DynMsgHandler<MsgT>>>>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        let actor_name = Arc::new(name.into());
        let actor_name_clone = actor_name.clone();
        let actor_node_id = node_id.into();
        let actor_system_name = self.async_runtime.name.clone();
        self.spawn_thread_blocking_task(move || {
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
        ThreadActorData {
            tx,
            handler_tx,
            close_cond,
            name: actor_name_clone,
        }
    }

    fn push_async_task(&mut self, tokio_join_handle: TokioJoinHandle<()>) {
        push_async_task(&mut self.async_tasks, tokio_join_handle);
    }
}

impl Drop for ThreadActorSystem {
    fn drop(&mut self) {
        if self.join_tasks_on_drop {
            log::debug!(
                "Thread actor system '{}' dropping, joining tasks",
                self.async_runtime.name
            );
            join_tasks(self.async_runtime.clone().as_ref(), self.extract_tasks());
        } else {
            log::debug!(
                "Thread actor system '{}' dropping, not joining tasks",
                self.async_runtime.name
            );
        }
    }
}

impl Task for ThreadActorSystem {
    fn is_finished(&self) -> bool {
        self.thread_blocking_tasks.iter().all(|h| h.is_finished())
    }
}

#[derive(Clone, Debug)]
pub struct ThreadActorSystemHandle {
    actor_system: Arc<Mutex<ThreadActorSystem>>,
}

impl ThreadActorSystemHandle {
    /// Owns the passed runtime, using it only if no contextual handle is available;
    ///  if `None` is passed, it creates a runtime with multi-threaded support,
    ///  CPU-based thread pool size and all features enabled.
    pub fn new_actor_system(
        name: impl Into<String>,
        tokio: Option<Runtime>,
        join_tasks_on_drop: bool,
    ) -> Self {
        ThreadActorSystemHandle {
            actor_system: Arc::new(Mutex::new(ThreadActorSystem {
                async_runtime: Arc::new(AsyncRuntime::new(name, tokio)),
                thread_blocking_tasks: Default::default(),
                async_tasks: Default::default(),
                join_tasks_on_drop,
            })),
        }
    }

    pub fn spawn_thread_blocking_task(&self, f: impl FnOnce() + Send + 'static) {
        self.actor_system
            .lock()
            .unwrap()
            .spawn_thread_blocking_task(f);
    }
}

impl ActorSystemHandle for ThreadActorSystemHandle {
    type ActorRefT<MsgT>
        = ThreadActorRef<MsgT>
    where
        MsgT: ActorMsg;

    fn create<MsgT>(
        &self,
        node_id: impl Into<String>,
        name: impl Into<String>,
        join_on_drop: bool,
    ) -> Self::ActorRefT<MsgT>
    where
        MsgT: ActorMsg,
    {
        ThreadActorRef {
            actor: Arc::new(ThreadActor {
                data: self.actor_system.lock().unwrap().create(name, node_id),
                actor_system_handle: self.clone(),
                join_on_drop,
            }),
        }
    }
}

impl Task for ThreadActorSystemHandle {
    fn is_finished(&self) -> bool {
        self.actor_system.lock().unwrap().is_finished()
    }
}

impl Joinable<()> for ThreadActorSystemHandle {
    fn join(&mut self) {
        let mut actor_system_lock_guard = self.actor_system.lock().unwrap();
        let async_runtime = actor_system_lock_guard.async_runtime.clone();
        let tasks = actor_system_lock_guard.extract_tasks();
        // Drop the lock before joining tasks to avoid deadlocks if they also lock the actor system
        drop(actor_system_lock_guard);
        join_tasks(async_runtime.as_ref(), tasks);
    }
}
