use bft_grid_core::{ActorControl, ActorRef, ActorSystem, Joinable, TypedHandler};
use bft_grid_mt::{ThreadActorSystem, TokioActorSystem};

struct Actor1ToActor2(Box<dyn ActorRef<(), Actor1>>);

struct Actor1 {
    self_ref: Box<dyn ActorRef<(), Actor1>>,
    actor2_ref: Box<dyn ActorRef<Actor1ToActor2, Actor2>>,
    ping_count: u8,
}

struct Actor2 {}

impl TypedHandler<'_> for Actor2 {
    type MsgT = Actor1ToActor2;

    fn receive(&mut self, mut msg: Self::MsgT) -> Option<ActorControl> {
        println!("Actor2 received ref, sending ping to it");
        msg.0.send((), None);
        println!("Actor2 sent ping, exiting");
        Some(ActorControl::Exit())
    }
}

impl TypedHandler<'_> for Actor1 {
    type MsgT = ();

    fn receive(&mut self, _msg: ()) -> Option<ActorControl> {
        let ret = match self.ping_count {
            0 => {
                println!("Actor1 received first ping, sending ref to Actor2");
                let self_ref = self.self_ref.new_ref();
                self.actor2_ref.send(Actor1ToActor2(self_ref), None);
                None
            }
            1 => {
                println!("Actor1 received second ping, self-pinging");
                self.self_ref.send((), None);
                None
            }
            _ => {
                println!("Actor1 received third ping, exiting");
                Some(ActorControl::Exit())
            }
        };
        self.ping_count += 1;
        ret
    }
}

fn main() {
    let mut tokio_actor_system = TokioActorSystem::new();
    let mut async_actor_ref = tokio_actor_system.create("node".into(), "actor2".into());
    tokio_actor_system.set_handler(&mut async_actor_ref, Actor2 {});
    let async_actor_ref2 = async_actor_ref.new_ref();
    let mut thread_actor_system = ThreadActorSystem {};
    let mut sync_actor_ref = thread_actor_system.create("node".into(), "actor1".into());
    let sync_actor_ref2 = sync_actor_ref.new_ref();
    thread_actor_system.set_handler(
        &mut sync_actor_ref,
        Actor1 {
            self_ref: sync_actor_ref2,
            actor2_ref: async_actor_ref2,
            ping_count: 0,
        },
    );
    sync_actor_ref.send((), None);
    async_actor_ref.join();
    println!("Joined Actor2");
    sync_actor_ref.join();
    println!("Joined Actor1");
}
