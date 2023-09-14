use bft_grid_core::{MessageHandler, ModuleSystem};
use bft_grid_tokio::{TokioModuleSystem, ThreadModuleSystem};

struct TestHandler{}

impl MessageHandler<()> for TestHandler {
    fn receive(&mut self, _: ()) -> () {
        println!("Received")
    }
}

#[tokio::main]
async fn main() {
    let mut async_actor_ref = TokioModuleSystem::spawn(TestHandler{});
    async_actor_ref.async_send(());
    let mut sync_actor_ref = ThreadModuleSystem::spawn(TestHandler{});
    sync_actor_ref.async_send(());
}
