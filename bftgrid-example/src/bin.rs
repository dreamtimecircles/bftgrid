use std::marker::PhantomData;

use bftgrid_core::{ActorControl, ActorMsg, ActorRef, ActorSystem, Joinable, TypedHandler};
use bftgrid_mt::{ThreadActorSystem, TokioActorSystem};

#[derive(Clone, Debug)]
struct Ping();
impl ActorMsg for Ping {}

#[derive(Clone, Debug)]
struct Actor1ToActor2<ActorSystemT>
where
    ActorSystemT: ActorSystem + 'static,
{
    pub actor1_ref: Box<dyn ActorRef<Ping, Actor1<ActorSystemT>>>,
}

impl<ActorSystemT> ActorMsg for Actor1ToActor2<ActorSystemT> where
    ActorSystemT: ActorSystem + 'static
{
}

#[derive(Debug)]
struct Actor1<ActorSystemT>
where
    ActorSystemT: ActorSystem + 'static,
{
    self_ref: Box<dyn ActorRef<Ping, Actor1<ActorSystemT>>>,
    node_id: String,
    actor_system: ActorSystemT,
    actor2_ref: Box<dyn ActorRef<Actor1ToActor2<ActorSystemT>, Actor2<ActorSystemT>>>,
    ping_count: u8,
    spawn_count: u8,
}

impl<ActorSystemT> Actor1<ActorSystemT>
where
    ActorSystemT: ActorSystem,
{
    fn new(
        self_ref: Box<dyn ActorRef<Ping, Actor1<ActorSystemT>>>,
        node_id: String,
        actor_system: ActorSystemT,
        actor2_ref: Box<dyn ActorRef<Actor1ToActor2<ActorSystemT>, Actor2<ActorSystemT>>>,
    ) -> Actor1<ActorSystemT> {
        Actor1 {
            self_ref,
            node_id,
            actor_system,
            actor2_ref,
            ping_count: 0,
            spawn_count: 0,
        }
    }
}

#[derive(Debug)]
struct Actor2<Actor1ActorSystemT>
where
    Actor1ActorSystemT: ActorSystem,
{
    actor_system_type: PhantomData<Actor1ActorSystemT>,
}

impl<Actor1ActorSystemT> Actor2<Actor1ActorSystemT>
where
    Actor1ActorSystemT: ActorSystem,
{
    fn new() -> Self {
        Actor2 {
            actor_system_type: PhantomData {},
        }
    }
}

impl<Actor1ActorSystemT> TypedHandler<'_> for Actor2<Actor1ActorSystemT>
where
    Actor1ActorSystemT: ActorSystem + 'static,
{
    type MsgT = Actor1ToActor2<Actor1ActorSystemT>;

    fn receive(&mut self, mut msg: Self::MsgT) -> Option<ActorControl> {
        println!("Actor2 received ref, sending ping to it");
        msg.actor1_ref.send(Ping(), None);
        println!("Actor2 sent ping, exiting");
        Some(ActorControl::Exit())
    }
}

impl<ActorSystemT> TypedHandler<'_> for Actor1<ActorSystemT>
where
    ActorSystemT: ActorSystem + 'static,
{
    type MsgT = Ping;

    fn receive(&mut self, _msg: Ping) -> Option<ActorControl> {
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
                self.self_ref.send(Ping(), None);
                None
            }
            _ => {
                println!("Actor1 received third ping");
                if self.spawn_count < 1 {
                    println!("Actor1 spawning");
                    let mut new_ref = self.actor_system.create::<Ping, Actor1<ActorSystemT>>(
                        self.node_id.clone(),
                        self.spawn_count.to_string(),
                    );
                    println!("Actor1 setting handler");
                    self.actor_system.set_handler(
                        &mut new_ref,
                        Actor1 {
                            self_ref: self.self_ref.new_ref(),
                            node_id: self.node_id.clone(),
                            actor_system: self.actor_system.clone(),
                            actor2_ref: self.actor2_ref.new_ref(),
                            ping_count: 3,
                            spawn_count: self.spawn_count + 1,
                        },
                    );
                    println!("Actor1 sending");
                    new_ref.send(Ping(), None);
                    println!("Actor1 done");
                }
                println!("Actor1 exiting");
                Some(ActorControl::Exit())
            }
        };
        self.ping_count += 1;
        ret
    }
}

struct System<Actor1ActorSystemT, Actor2ActorSystemT>
where
    Actor1ActorSystemT: ActorSystem + 'static,
    Actor2ActorSystemT: ActorSystem + 'static,
{
    actor1_ref: Actor1ActorSystemT::ActorRefT<Ping, Actor1<Actor1ActorSystemT>>,
    actor2_ref: Actor2ActorSystemT::ActorRefT<
        Actor1ToActor2<Actor1ActorSystemT>,
        Actor2<Actor1ActorSystemT>,
    >,
    actor1_actor_system_type: PhantomData<Actor1ActorSystemT>,
}

impl<Actor1ActorSystemT, Actor2ActorSystemT> System<Actor1ActorSystemT, Actor2ActorSystemT>
where
    Actor1ActorSystemT: ActorSystem + 'static,
    Actor2ActorSystemT: ActorSystem + 'static,
{
    fn new(
        actor1_ref: Actor1ActorSystemT::ActorRefT<Ping, Actor1<Actor1ActorSystemT>>,
        actor2_ref: Actor2ActorSystemT::ActorRefT<
            Actor1ToActor2<Actor1ActorSystemT>,
            Actor2<Actor1ActorSystemT>,
        >,
    ) -> Self {
        System {
            actor1_ref,
            actor2_ref,
            actor1_actor_system_type: PhantomData {},
        }
    }
}

fn build_system<Actor1ActorSystemT, Actor2ActorSystemT>(
    mut actor1_actor_system: Actor1ActorSystemT,
    mut actor2_actor_system: Actor2ActorSystemT,
) -> System<Actor1ActorSystemT, Actor2ActorSystemT>
where
    Actor1ActorSystemT: ActorSystem + 'static,
    Actor2ActorSystemT: ActorSystem + 'static,
{
    let mut actor1_ref = actor1_actor_system.create("node".into(), "actor1".into());
    let actor1_ref_copy = actor1_ref.new_ref();
    let mut actor2_ref = actor2_actor_system.create("node".into(), "actor2".into());
    let actor2_ref_copy = actor2_ref.new_ref();
    actor2_actor_system.set_handler(&mut actor2_ref, Actor2::new());
    actor1_actor_system.set_handler(
        &mut actor1_ref,
        Actor1::new(
            actor1_ref_copy,
            "node".into(),
            actor1_actor_system.clone(),
            actor2_ref_copy,
        ),
    );
    System::new(actor1_ref, actor2_ref)
}

fn main() {
    let thread_actor_system = ThreadActorSystem::new();
    let tokio_actor_system = TokioActorSystem::new();
    let System {
        mut actor1_ref,
        actor2_ref,
        ..
    } = build_system(thread_actor_system.clone(), tokio_actor_system.clone());
    actor1_ref.send(Ping(), None);
    actor2_ref.join();
    println!("Joined Actor2");
    actor1_ref.join();
    println!("Joined Actor1");
    tokio_actor_system.join();
    thread_actor_system.join();
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        ops::Add,
        time::{Duration, Instant},
    };

    use bftgrid_core::{ActorRef, P2PNode};
    use bftgrid_sim::Simulation;

    use crate::{build_system, Ping, System};

    #[test]
    fn simulation() {
        let mut topology = HashMap::new();
        topology.insert("node".into(), P2PNode::new());
        let start = Instant::now();
        let simulation = Simulation::new(topology, start, start.add(Duration::from_secs(100)));
        let System { mut actor1_ref, .. } = build_system(simulation.clone(), simulation.clone());
        actor1_ref.send(Ping(), None);
        let history = simulation.run();
        println!("{:?}", history);
    }
}
