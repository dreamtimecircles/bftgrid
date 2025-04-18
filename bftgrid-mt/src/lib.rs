use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex},
};

use bftgrid_core::Task;

pub mod thread;
pub mod tokio;

#[derive(Debug)]
enum TokioRuntimeOrHandle {
    Runtime(::tokio::runtime::Runtime),
    Handle(::tokio::runtime::Handle),
}

#[derive(Debug)]
pub struct TokioRuntime {
    pub name: Arc<String>,
    value: TokioRuntimeOrHandle,
}

/// Unified interface for awaiting both async tasks and thread-blocking tasks
/// from any context.
impl TokioRuntime {
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
                match &self.value {
                    TokioRuntimeOrHandle::Runtime(runtime) => runtime.block_on(f),
                    TokioRuntimeOrHandle::Handle(handle) => handle.block_on(f),
                }
            }
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

    pub fn spawn<R>(
        &self,
        f: impl Future<Output = R> + Send + 'static,
    ) -> ::tokio::task::JoinHandle<R>
    where
        R: Send + 'static,
    {
        match &self.value {
            TokioRuntimeOrHandle::Runtime(runtime) => runtime.spawn(f),
            TokioRuntimeOrHandle::Handle(handle) => handle.spawn(f),
        }
    }
}

/// Creates a new Tokio runtime or reuses the existing one if already present,
///  as to avoid closing a new runtime from an async context (which panics).
///  If a runtime is created, it has multi-threaded support,
///  CPU-based thread pool size and all features enabled.
pub fn get_tokio_runtime(name: impl Into<String>) -> TokioRuntime {
    let runtime_name = Arc::new(name.into());
    TokioRuntime {
        name: runtime_name.clone(),
        value: match ::tokio::runtime::Handle::try_current() {
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
