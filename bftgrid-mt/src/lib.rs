use std::sync::{Arc, Condvar, Mutex};

use bftgrid_core::Task;

pub mod thread;
pub mod tokio;

#[derive(Debug)]
pub struct TokioRuntime {
    pub name: Arc<String>,
    pub underlying: ::tokio::runtime::Runtime,
}

pub fn new_tokio_runtime(name: impl Into<String>) -> TokioRuntime {
    TokioRuntime {
        name: name.into().into(),
        underlying: ::tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
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
