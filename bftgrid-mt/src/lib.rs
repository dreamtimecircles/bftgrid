use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex, RwLock},
    time::Duration,
};

use bftgrid_core::actor::{ActorMsg, AnActorRef, Task, TypedHandler};

pub mod thread;
pub mod tokio;

/// Offers a unified interface for awaiting both async tasks and thread-blocking tasks
///  from any context.
///
/// [`Clone`] is intentionally not implemented so that an [`AsyncRuntime`]
///  can only be shared explicitly, as the destructor needs to shut down the
///  underlying owned Tokio runtime, so it needs to be called only once all references are dropped.
///
/// Dropping an ['AsyncRuntime'] also extracts and shuts down the underlying owned Tokio runtime
///  without waiting for tasks to finish. This allows dropping it also from async contexts,
///  but users may want to ensure that all tasks are finished beforehand, if leaks
///  may otherwise occur, for example if dropping occurs mid-program and not at the end.
#[derive(Debug)]
pub struct AsyncRuntime {
    pub name: Arc<String>,
    // Using a lock so that the field can be written to extract the runtime
    //  and then shut it down on drop without waiting for the tasks to finish,
    //  which allows dropping it also from async contexts.
    //
    // Using a RwLock instead of a Mutex allows to do so without requiring lock
    //  exclusivity for normal operations (which only require read access);
    //  this avoids likely deadlocks, as an `AsyncRuntime` is often shared.
    tokio: Arc<RwLock<Option<::tokio::runtime::Runtime>>>,
}

impl Drop for AsyncRuntime {
    fn drop(&mut self) {
        log::debug!("Dropping Tokio runtime '{}'", self.name);
        // Extract the tokio runtime from the RwLock and shut it down
        if let Some(tokio) = self.tokio.write().unwrap().take() {
            tokio.shutdown_background();
        }
    }
}

impl AsyncRuntime {
    /// Owns the passed runtime, using it only if no contextual handle is available;
    ///  if `None` is passed, it creates a runtime with multi-threaded support,
    ///  CPU-based thread pool size and all features enabled.
    pub fn new(name: impl Into<String>, tokio: Option<::tokio::runtime::Runtime>) -> AsyncRuntime {
        let runtime_name = Arc::new(name.into());
        AsyncRuntime {
            name: runtime_name.clone(),
            tokio: Arc::new(RwLock::new(tokio.or({
                log::debug!("Creating new Tokio runtime as '{}'", runtime_name);
                Some(
                    ::tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .unwrap(),
                )
            }))),
        }
    }

    pub fn await_async<R>(&self, f: impl Future<Output = R>) -> R {
        match ::tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                log::debug!(
                    "Tokio runtime '{}' blocking on async inside an async context",
                    self.name,
                );
                let _guard = handle.enter();
                ::tokio::task::block_in_place(|| handle.block_on(f))
            }
            _ => {
                log::debug!(
                    "Tokio runtime '{}' blocking on async outside of an async context",
                    self.name,
                );
                self.tokio.read().unwrap().as_ref().unwrap().block_on(f)
            }
        }
    }

    pub fn spawn_async<R>(
        &self,
        f: impl Future<Output = R> + Send + 'static,
    ) -> ::tokio::task::JoinHandle<R>
    where
        R: Send + 'static,
    {
        match ::tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.spawn(f),
            _ => self.tokio.read().unwrap().as_ref().unwrap().spawn(f),
        }
    }

    pub fn thread_blocking<R>(&self, f: impl FnOnce() -> R) -> R {
        match ::tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                log::debug!(
                    "Tokio runtime '{}' blocking thread inside an async context",
                    self.name,
                );
                let _guard = handle.enter();
                ::tokio::task::block_in_place(f)
            }
            _ => {
                log::debug!(
                    "Tokio runtime '{}' blocking thread outside of an async context",
                    self.name,
                );
                f()
            }
        }
    }

    pub fn spawn_async_send<MsgT, HandlerT, O>(
        &self,
        f: impl Future<Output = O> + Send + 'static,
        to_msg: impl FnOnce(O) -> MsgT + Send + 'static,
        mut actor_ref: AnActorRef<MsgT, HandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
    {
        match ::tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.spawn(async move {
                actor_ref.send(to_msg(f.await), delay);
            }),
            _ => self
                .tokio
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .spawn(async move {
                    actor_ref.send(to_msg(f.await), delay);
                }),
        };
    }

    pub fn thread_blocking_send<MsgT, HandlerT, R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
        to_msg: impl FnOnce(R) -> MsgT + Send + 'static,
        mut actor_ref: AnActorRef<MsgT, HandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
        R: Send + 'static,
    {
        match ::tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                log::debug!(
                    "Tokio runtime '{}' performing blocking and then send inside an async context",
                    self.name,
                );
                let _guard = handle.enter();
                let actor_system_name = self.name.clone();
                self.spawn_async(async move {
                    match ::tokio::task::spawn_blocking(f).await {
                        Ok(result) => actor_ref.send(to_msg(result), delay),
                        Err(_) => log::error!(
                            "Tokio runtime '{}': blocking task failed",
                            actor_system_name
                        ),
                    };
                });
            }
            _ => {
                log::debug!(
                    "Tokio runtime '{}' performing blocking and then send outside of an async context",
                    self.name,
                );
                actor_ref.send(to_msg(f()), delay)
            }
        }
    }
}

fn notify_close(close_cond: Arc<(Mutex<bool>, Condvar)>) {
    let (closed_mutex, cvar) = &*close_cond;
    let mut closed = closed_mutex.lock().unwrap();
    *closed = true;
    cvar.notify_all();
}

fn cleanup_complete_tasks<TaskT>(tasks: &mut Vec<TaskT>) -> &mut Vec<TaskT>
where
    TaskT: Task,
{
    tasks.retain(|t| !t.is_finished());
    tasks
}
