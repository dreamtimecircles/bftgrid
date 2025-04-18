use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use bftgrid_core::{ActorMsg, AnActorRef, Task, TypedHandler};

pub mod thread;
pub mod tokio;

#[derive(Debug)]
enum TokioRuntimeOrHandle {
    Runtime(::tokio::runtime::Runtime),
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
            Err(_) => {
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
        match &self.tokio {
            TokioRuntimeOrHandle::Runtime(runtime) => runtime.spawn(f),
            TokioRuntimeOrHandle::Handle(handle) => handle.spawn(f),
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
            Err(_) => {
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
        match &self.tokio {
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
            Err(_) => {
                log::debug!(
                    "Tokio runtime '{}' performing blocking and then send outside of an async context",
                    self.name,
                );
                actor_ref.send(to_msg(f()), delay)
            }
        }
    }
}

/// Creates a new Tokio runtime or reuses the existing one if already present,
///  as to avoid closing a new runtime from an async context (which panics).
///  If a runtime is created, it has multi-threaded support,
///  CPU-based thread pool size and all features enabled.
pub fn get_async_runtime(name: impl Into<String>) -> AsyncRuntime {
    let runtime_name = Arc::new(name.into());
    AsyncRuntime {
        name: runtime_name.clone(),
        tokio: match ::tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                log::debug!("Reusing existing Tokyio runtime as '{}'", runtime_name,);
                TokioRuntimeOrHandle::Handle(handle)
            }
            Err(_) => {
                log::debug!("Creating new Tokio runtime as '{}'", runtime_name,);
                TokioRuntimeOrHandle::Runtime(
                    ::tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .unwrap(),
                )
            }
        },
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
