use std::borrow::Cow;

use bft_grid_core::{ActorSystem, TypedMessageHandler};
use bft_grid_mt::{ThreadActorSystem, TokioActorSystem};

struct TestHandler {}

impl TypedMessageHandler<'_> for TestHandler {
    type Msg = ();

    fn receive(&mut self, _: ()) -> () {
        println!("Received")
    }
}

#[tokio::main]
async fn main() {
    let mut tokio_actor_system = TokioActorSystem {};
    let mut async_actor_ref = tokio_actor_system.spawn_actor("test1".into(), TestHandler {});
    async_actor_ref.async_send(());
    let mut thread_actor_system = ThreadActorSystem {};
    let mut sync_actor_ref = thread_actor_system.spawn_actor("test1".into(), TestHandler {});
    sync_actor_ref.async_send(());
}
