use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use bftgrid_core::actor::{ActorMsg, AnActorRef, Task, TypedHandler};

pub mod thread;
pub mod tokio;

#[derive(Debug)]
pub enum TokioRuntimeOrHandle {
    Runtime(Arc<::tokio::runtime::Runtime>),
    Handle(::tokio::runtime::Handle),
}

#[derive(Debug)]
pub struct AsyncRuntime {
    pub name: Arc<String>,
    tokio: TokioRuntimeOrHandle,
}

/// Unified interface for awaiting both async tasks and thread-blocking tasks
/// from any context.
impl AsyncRuntime {
    /// Caches the passed runtime or handle, else the contextual handle,
    ///  if available, else it creates a runtime with multi-threaded support,
    ///  CPU-based thread pool size and all features enabled.
    ///
    /// The cached runtime or handle are used only if no contextual handle is available.
    ///
    /// As generally for Tokio, anything that owns a runtime cannot be dropped
    ///  from an async context.
    pub fn new(name: impl Into<String>, tokio: Option<TokioRuntimeOrHandle>) -> AsyncRuntime {
        let runtime_name = Arc::new(name.into());
        AsyncRuntime {
            name: runtime_name.clone(),
            tokio: tokio.unwrap_or(match ::tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    log::debug!("Reusing existing Tokyio runtime as '{}'", runtime_name,);
                    TokioRuntimeOrHandle::Handle(handle)
                }
                _ => {
                    log::debug!("Creating new Tokio runtime as '{}'", runtime_name,);
                    TokioRuntimeOrHandle::Runtime(Arc::new(
                        ::tokio::runtime::Builder::new_multi_thread()
                            .enable_all()
                            .build()
                            .unwrap(),
                    ))
                }
            }),
        }
    }

    pub fn await_async<R>(&self, f: impl Future<Output = R>) -> R {
        match self.get_runtime_or_handle() {
            TokioRuntimeOrHandle::Handle(handle) => {
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
                match &self.tokio {
                    TokioRuntimeOrHandle::Runtime(runtime) => runtime.block_on(f),
                    TokioRuntimeOrHandle::Handle(handle) => handle.block_on(f),
                }
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
        match self.get_runtime_or_handle() {
            TokioRuntimeOrHandle::Runtime(runtime) => runtime.spawn(f),
            TokioRuntimeOrHandle::Handle(handle) => handle.spawn(f),
        }
    }

    pub fn thread_blocking<R>(&self, f: impl FnOnce() -> R) -> R {
        match self.get_runtime_or_handle() {
            TokioRuntimeOrHandle::Handle(handle) => {
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
        match self.get_runtime_or_handle() {
            TokioRuntimeOrHandle::Runtime(runtime) => runtime.spawn(async move {
                actor_ref.send(to_msg(f.await), delay);
            }),
            TokioRuntimeOrHandle::Handle(handle) => handle.spawn(async move {
                actor_ref.send(to_msg(f.await), delay);
            }),
        };
    }

    pub fn spawn_blocking_send<MsgT, HandlerT, R>(
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
        match self.get_runtime_or_handle() {
            TokioRuntimeOrHandle::Handle(handle) => {
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

    fn get_runtime_or_handle(&self) -> TokioRuntimeOrHandle {
        match ::tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                log::debug!(
                    "Async runtime '{}' using contextual Tokio runtime handle",
                    self.name,
                );
                TokioRuntimeOrHandle::Handle(handle)
            }
            Err(_) => match &self.tokio {
                TokioRuntimeOrHandle::Runtime(runtime) => {
                    log::debug!("Tokio runtime '{}' using existing runtime", self.name,);
                    TokioRuntimeOrHandle::Runtime(runtime.clone())
                }
                TokioRuntimeOrHandle::Handle(handle) => {
                    TokioRuntimeOrHandle::Handle(handle.clone())
                }
            },
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
