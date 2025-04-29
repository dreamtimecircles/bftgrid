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

use bftgrid_core::actor::{
    ActorControl, ActorMsg, ActorRef, ActorSystemHandle, AnActorRef, Joinable, Task,
    TypedMsgHandler,
};
use tokio::runtime::Runtime;

use crate::{
    cleanup_complete_tasks, join_tasks, notify_close, push_async_task, AsyncRuntime,
    ThreadJoinable, TokioTask,
};

#[derive(Debug)]
pub struct ThreadActorData<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    tx: Sender<MsgT>,
    handler_tx: Sender<Arc<Mutex<MsgHandlerT>>>,
    close_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl<MsgT, MsgHandlerT> Clone for ThreadActorData<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn clone(&self) -> Self {
        ThreadActorData {
            tx: self.tx.clone(),
            handler_tx: self.handler_tx.clone(),
            close_cond: self.close_cond.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ThreadActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    data: ThreadActorData<MsgT, MsgHandlerT>,
    actor_system: ThreadActorSystemHandle,
}

impl<MsgT, MsgHandlerT> Task for ThreadActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn is_finished(&self) -> bool {
        let (closed_mutex, _) = &*self.data.close_cond;
        *closed_mutex.lock().unwrap()
    }
}

impl<MsgT, MsgHandlerT> Joinable<()> for ThreadActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn join(&mut self) {
        let (close_mutex, cvar) = &*self.data.close_cond;
        let mut closed = close_mutex.lock().unwrap();
        while !*closed {
            closed = cvar.wait(closed).unwrap();
        }
    }
}

impl<MsgT, MsgHandlerT> ActorRef<MsgT, MsgHandlerT> for ThreadActor<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        let sender = self.data.tx.clone();
        if let Some(delay_duration) = delay {
            self.actor_system.spawn_thread_blocking_task(move || {
                log::debug!("Delaying send by {:?}", delay_duration);
                thread::sleep(delay_duration);
                checked_send(sender, message);
            });
        } else {
            // No need to spawn a thread if no delay is needed, as the sender is non-blocking
            checked_send(sender, message);
        }
    }

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, MsgHandlerT>> {
        Box::new(ThreadActor {
            data: self.data.clone(),
            actor_system: self.actor_system.clone(),
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

    fn create<MsgT, MsgHandlerT>(
        &mut self,
        name: impl Into<String>,
        node_id: impl Into<String>,
    ) -> ThreadActorData<MsgT, MsgHandlerT>
    where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let (handler_tx, handler_rx) = mpsc::channel::<Arc<Mutex<MsgHandlerT>>>();
        let close_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let close_cond2 = close_cond.clone();
        let actor_name = name.into();
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
        }
    }

    fn set_handler<MsgT, MsgHandlerT>(
        &mut self,
        actor_ref: &mut ThreadActor<MsgT, MsgHandlerT>,
        handler: MsgHandlerT,
    ) where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
    {
        actor_ref
            .data
            .handler_tx
            .send(Arc::new(Mutex::new(handler)))
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
            self.async_runtime.spawn_async_send(f, actor_ref, delay),
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
            self.async_runtime
                .spawn_thread_blocking_send(f, actor_ref, delay),
        );
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

    pub fn spawn_thread_blocking_task(&mut self, f: impl FnOnce() + Send + 'static) {
        self.actor_system
            .lock()
            .unwrap()
            .spawn_thread_blocking_task(f);
    }
}

impl ActorSystemHandle for ThreadActorSystemHandle {
    type ActorRefT<MsgT, MsgHandlerT>
        = ThreadActor<MsgT, MsgHandlerT>
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
        ThreadActor {
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
