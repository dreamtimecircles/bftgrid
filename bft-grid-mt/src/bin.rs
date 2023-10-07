use std::{thread, time::Duration};

use bft_grid_core::{ActorRef, ActorSystem, TypedMessageHandler};
use bft_grid_mt::{ThreadActorSystem, TokioActorSystem};

struct Actor1ToActor2();

struct Actor1 {
    actor2_ref: Box<dyn ActorRef<Actor1ToActor2> + Send>,
}

struct Actor2 {}

impl TypedMessageHandler<'_> for Actor2 {
    type Msg = Actor1ToActor2;

    fn receive(&mut self, _msg: Actor1ToActor2) -> () {
        println!("Received")
    }
}

impl TypedMessageHandler<'_> for Actor1 {
    type Msg = ();

    fn receive(&mut self, _msg: ()) -> () {
        self.actor2_ref.async_send(Actor1ToActor2())
    }
}

#[tokio::main]
async fn main() {
    let mut tokio_actor_system = TokioActorSystem {};
    let async_actor_ref = tokio_actor_system.spawn_actor("test1".into(), Actor2 {});
    let mut thread_actor_system = ThreadActorSystem {};
    let sync_actor_ref = thread_actor_system.spawn_actor(
        "test1".into(),
        Actor1 {
            actor2_ref: async_actor_ref,
        },
    );
    sync_actor_ref.async_send(());
    thread::sleep(Duration::from_secs(1));
}
