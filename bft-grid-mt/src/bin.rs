use bft_grid_core::{
    ActorControl, ActorRef, ActorSystem, Joinable, SingleThreadedActorRef, TypedMessageHandler,
};
use bft_grid_mt::{ThreadActorSystem, TokioActorSystem};

struct Actor1ToActor2();

struct Actor1 {
    actor2_ref: Box<dyn ActorRef<Actor1ToActor2, Actor2>>,
}

struct Actor2 {}

impl TypedMessageHandler<'_> for Actor2 {
    type Msg = Actor1ToActor2;

    fn receive(&mut self, _msg: Self::Msg) -> Option<ActorControl> {
        println!("Received");
        Some(ActorControl::Exit())
    }
}

impl TypedMessageHandler<'_> for Actor1 {
    type Msg = ();

    fn receive(&mut self, _msg: ()) -> Option<ActorControl> {
        self.actor2_ref.send(Actor1ToActor2(), None);
        Some(ActorControl::Exit())
    }
}

fn main() {
    let mut tokio_actor_system = TokioActorSystem::new();
    let mut async_actor_ref = tokio_actor_system.create("node".into(), "test1".into());
    tokio_actor_system.set_handler(&mut async_actor_ref, Actor2 {});
    let async_actor_ref2 = async_actor_ref.clone();
    let mut thread_actor_system = ThreadActorSystem {};
    let mut sync_actor_ref = thread_actor_system.create("node".into(), "test2".into());
    thread_actor_system.set_handler(
        &mut sync_actor_ref,
        Actor1 {
            actor2_ref: async_actor_ref2,
        },
    );
    sync_actor_ref.send((), None);
    async_actor_ref.join();
    println!("Joined 1");
    sync_actor_ref.join();
    println!("Joined 2");
}
