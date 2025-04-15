use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use bftgrid_core::Task;

pub mod thread;
pub mod tokio;

fn notify_close(close_cond: Arc<(Mutex<bool>, Condvar)>) {
    let (closed_mutex, cvar) = &*close_cond;
    let mut closed = closed_mutex.lock().unwrap();
    *closed = true;
    cvar.notify_all();
}

fn cleanup_complete_tasks<TaskT>(
    mut tasks: MutexGuard<'_, Vec<TaskT>>,
) -> MutexGuard<'_, Vec<TaskT>>
where
    TaskT: Task,
{
    tasks.retain(|t| !t.is_finished());
    tasks
}
