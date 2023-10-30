use std::marker::PhantomData;

use bft_grid_core::{ActorControl, ActorRef, ActorSystem, Joinable, TypedHandler};
use bft_grid_mt::{ThreadActorSystem, TokioActorSystem};

struct Actor1ToActor2<ActorSystemT>
where
    ActorSystemT: ActorSystem,
{
    pub actor1_ref: Box<dyn ActorRef<(), Actor1<ActorSystemT>>>,
}

struct Actor1<ActorSystemT>
where
    ActorSystemT: ActorSystem,
{
    self_ref: Box<dyn ActorRef<(), Actor1<ActorSystemT>>>,
    actor_system: ActorSystemT,
    actor2_ref: Box<dyn ActorRef<Actor1ToActor2<ActorSystemT>, Actor2<ActorSystemT>>>,
    ping_count: u8,
    spawn_count: u8,
}

struct Actor2<ActorSystemT>
where
    ActorSystemT: ActorSystem,
{
    actor_system_type: PhantomData<ActorSystemT>,
}

impl<ActorSystemT> Actor2<ActorSystemT>
where
    ActorSystemT: ActorSystem,
{
    fn new() -> Self {
        Actor2 {
            actor_system_type: PhantomData {},
        }
    }
}

impl<ActorSystemT> TypedHandler<'_> for Actor2<ActorSystemT>
where
    ActorSystemT: ActorSystem + Send + 'static,
{
    type MsgT = Actor1ToActor2<ActorSystemT>;

    fn receive(&mut self, mut msg: Self::MsgT) -> Option<ActorControl> {
        println!("Actor2 received ref, sending ping to it");
        msg.actor1_ref.send((), None);
        println!("Actor2 sent ping, exiting");
        Some(ActorControl::Exit())
    }
}

impl<ActorSystemT> TypedHandler<'_> for Actor1<ActorSystemT>
where
    ActorSystemT: ActorSystem + Send + 'static,
{
    type MsgT = ();

    fn receive(&mut self, _msg: ()) -> Option<ActorControl> {
        let ret = match self.ping_count {
            0 => {
                println!("Actor1 received first ping, sending ref to Actor2");
                let self_ref = self.self_ref.new_ref();
                self.actor2_ref.send(
                    Actor1ToActor2 {
                        actor1_ref: self_ref,
                    },
                    None,
                );
                None
            }
            1 => {
                println!("Actor1 received second ping, self-pinging");
                self.self_ref.send((), None);
                None
            }
            _ => {
                println!("Actor1 received third ping");
                if self.spawn_count < 1 {
                    println!("Actor1 spawning");
                    let mut new_ref = self
                        .actor_system
                        .create::<(), Actor1<ActorSystemT>>("".into(), "".into());
                    self.actor_system.set_handler(
                        &mut new_ref,
                        Actor1 {
                            self_ref: self.self_ref.new_ref(),
                            actor_system: self.actor_system.clone(),
                            actor2_ref: self.actor2_ref.new_ref(),
                            ping_count: 3,
                            spawn_count: self.spawn_count + 1,
                        },
                    );
                    new_ref.send((), None);
                    println!("Actor1 joining");
                    new_ref.join();
                }
                println!("Actor1 exiting");
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
    tokio_actor_system.set_handler(&mut async_actor_ref, Actor2::new());
    let async_actor_ref2 = async_actor_ref.new_ref();
    let mut thread_actor_system = ThreadActorSystem {};
    let mut sync_actor_ref = thread_actor_system.create("node".into(), "actor1".into());
    let sync_actor_ref2 = sync_actor_ref.new_ref();
    thread_actor_system.set_handler(
        &mut sync_actor_ref,
        Actor1 {
            self_ref: sync_actor_ref2,
            actor_system: thread_actor_system.clone(),
            actor2_ref: async_actor_ref2,
            ping_count: 0,
            spawn_count: 0,
        },
    );
    sync_actor_ref.send((), None);
    async_actor_ref.join();
    println!("Joined Actor2");
    sync_actor_ref.join();
    println!("Joined Actor1");
}
