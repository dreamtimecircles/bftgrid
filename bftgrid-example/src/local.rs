mod utils;

use std::marker::PhantomData;

use bftgrid_core::actor::{
    ActorControl, ActorMsg, ActorRef, ActorSystemHandle, Joinable, TypedMsgHandler,
};
use bftgrid_mt::{thread::ThreadActorSystemHandle, tokio::TokioActorSystemHandle};

#[derive(Clone, Debug)]
struct Ping();
impl ActorMsg for Ping {}

#[derive(Clone, Debug)]
struct Actor1ToActor2<Actor1RefT>
where
    Actor1RefT: ActorRef<Ping>,
{
    pub actor1_ref: Actor1RefT,
}

impl<Actor1RefT> ActorMsg for Actor1ToActor2<Actor1RefT> where Actor1RefT: ActorRef<Ping> + 'static {}

#[derive(Debug)]
struct Actor1<ActorSystemT, Actor1RefT, Actor2RefT>
where
    ActorSystemT: ActorSystemHandle + 'static,
    Actor1RefT: ActorRef<Ping> + 'static,
    Actor2RefT: ActorRef<Actor1ToActor2<Actor1RefT>>,
{
    self_ref: Actor1RefT,
    node_id: String,
    actor_system: ActorSystemT,
    actor2_ref: Actor2RefT,
    ping_count: u8,
    spawn_count: u8,
}

impl<ActorSystemT, Actor1RefT, Actor2RefT> Actor1<ActorSystemT, Actor1RefT, Actor2RefT>
where
    ActorSystemT: ActorSystemHandle + std::fmt::Debug + Send,
    Actor1RefT: ActorRef<Ping> + 'static,
    Actor2RefT: ActorRef<Actor1ToActor2<Actor1RefT>>,
{
    fn new(
        self_ref: Actor1RefT,
        node_id: impl Into<String>,
        actor_system: ActorSystemT,
        actor2_ref: Actor2RefT,
    ) -> Actor1<ActorSystemT, Actor1RefT, Actor2RefT> {
        Actor1 {
            self_ref,
            node_id: node_id.into(),
            actor_system,
            actor2_ref,
            ping_count: 0,
            spawn_count: 0,
        }
    }
}

#[derive(Debug)]
struct Actor2<Actor1ActorSystemT, Actor1RefT>
where
    Actor1ActorSystemT: ActorSystemHandle,
    Actor1RefT: ActorRef<Ping>,
{
    _p1: PhantomData<Actor1ActorSystemT>,
    _p2: PhantomData<Actor1RefT>,
}

impl<Actor1ActorSystemT, Actor1RefT> Actor2<Actor1ActorSystemT, Actor1RefT>
where
    Actor1ActorSystemT: ActorSystemHandle,
    Actor1RefT: ActorRef<Ping>,
{
    fn new() -> Self {
        Actor2 {
            _p1: PhantomData {},
            _p2: PhantomData {},
        }
    }
}

impl<Actor1ActorSystemT, Actor1RefT> TypedMsgHandler<Actor1ToActor2<Actor1RefT>>
    for Actor2<Actor1ActorSystemT, Actor1RefT>
where
    Actor1ActorSystemT: ActorSystemHandle + std::fmt::Debug + Send + 'static,
    Actor1RefT: ActorRef<Ping> + 'static,
{
    fn receive(&mut self, mut msg: Actor1ToActor2<Actor1RefT>) -> Option<ActorControl> {
        log::info!("Actor2 received ref, sending ping to it");
        msg.actor1_ref.send(Ping(), None);
        log::info!("Actor2 sent ping, exiting");
        Some(ActorControl::Exit())
    }
}

impl<ActorSystemT, Actor1RefT, Actor2RefT> TypedMsgHandler<Ping>
    for Actor1<ActorSystemT, Actor1RefT, Actor2RefT>
where
    ActorSystemT: ActorSystemHandle + std::fmt::Debug + Send + 'static,
    Actor1RefT: ActorRef<Ping> + 'static,
    Actor2RefT: ActorRef<Actor1ToActor2<Actor1RefT>> + 'static,
{
    fn receive(&mut self, _msg: Ping) -> Option<ActorControl> {
        let ret = match self.ping_count {
            0 => {
                log::info!("Actor1 received first ping, sending ref to Actor2");
                let self_ref = self.self_ref.clone();
                self.actor2_ref.send(
                    Actor1ToActor2 {
                        actor1_ref: self_ref,
                    },
                    None,
                );
                None
            }
            1 => {
                log::info!("Actor1 received second ping, self-pinging after async work");
                self.actor_system.spawn_async_send(
                    async move {
                        log::info!("Actor1 simulating async work");
                        Ping()
                    },
                    self.self_ref.clone(),
                    None,
                );
                None
            }
            _ => {
                log::info!("Actor1 received third ping");
                log::info!("Actor1 simulating async work");
                if self.spawn_count < 1 {
                    log::info!("Actor1 spawning");
                    let mut new_ref = self.actor_system.create::<Ping>(
                        self.node_id.clone(),
                        self.spawn_count.to_string(),
                        false,
                    );
                    log::info!("Actor1 setting handler");
                    self.actor_system.set_handler(
                        &mut new_ref,
                        Box::new(Actor1 {
                            self_ref: self.self_ref.clone(),
                            node_id: self.node_id.clone(),
                            actor_system: self.actor_system.clone(),
                            actor2_ref: self.actor2_ref.clone(),
                            ping_count: 3,
                            spawn_count: self.spawn_count + 1,
                        }),
                    );
                    log::info!("Actor1 sending");
                    new_ref.send(Ping(), None);
                    log::info!("Actor1 done");
                }
                log::info!("Actor1 exiting");
                Some(ActorControl::Exit())
            }
        };
        self.ping_count += 1;
        ret
    }
}

struct System<Actor1ActorSystemT, Actor2ActorSystemT>
where
    Actor1ActorSystemT: ActorSystemHandle + std::fmt::Debug + Send + 'static,
    Actor2ActorSystemT: ActorSystemHandle + 'static,
{
    actor1_ref: Actor1ActorSystemT::ActorRefT<Ping>,
    actor2_ref: Actor2ActorSystemT::ActorRefT<Actor1ToActor2<Actor1ActorSystemT::ActorRefT<Ping>>>,
    actor1_actor_system_type: PhantomData<Actor1ActorSystemT>,
}

impl<Actor1ActorSystemT, Actor2ActorSystemT> System<Actor1ActorSystemT, Actor2ActorSystemT>
where
    Actor1ActorSystemT: ActorSystemHandle + std::fmt::Debug + Send + 'static,
    Actor2ActorSystemT: ActorSystemHandle + 'static,
{
    fn new(
        actor1_ref: Actor1ActorSystemT::ActorRefT<Ping>,
        actor2_ref: Actor2ActorSystemT::ActorRefT<
            Actor1ToActor2<Actor1ActorSystemT::ActorRefT<Ping>>,
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
    actor1_actor_system: Actor1ActorSystemT,
    actor2_actor_system: Actor2ActorSystemT,
) -> System<Actor1ActorSystemT, Actor2ActorSystemT>
where
    Actor1ActorSystemT: ActorSystemHandle + std::fmt::Debug + Send + 'static,
    Actor2ActorSystemT: ActorSystemHandle + 'static,
{
    let mut actor1_ref = actor1_actor_system.create("node", "actor1", false);
    let actor1_ref_copy = actor1_ref.clone();
    let mut actor2_ref = actor2_actor_system.create("node", "actor2", false);
    let actor2_ref_copy = actor2_ref.clone();
    actor2_actor_system.set_handler(
        &mut actor2_ref,
        Box::new(Actor2::<
            Actor1ActorSystemT,
            Actor1ActorSystemT::ActorRefT<Ping>,
        >::new()),
    );
    actor1_actor_system.set_handler(
        &mut actor1_ref,
        Box::new(Actor1::new(
            actor1_ref_copy,
            "node",
            actor1_actor_system.clone(),
            actor2_ref_copy,
        )),
    );
    System::new(actor1_ref, actor2_ref)
}

// Components that need a Tokio runtime will reuse the one from the async context, if any,
//  otherwise they will create a new one.
#[tokio::main]
async fn main() {
    utils::setup_logging(false);
    let mut thread_actor_system =
        ThreadActorSystemHandle::new_actor_system("thread-as", None, false);
    let mut tokio_actor_system = TokioActorSystemHandle::new_actor_system("tokio-as", None, false);
    let System {
        mut actor1_ref,
        mut actor2_ref,
        ..
    } = build_system(thread_actor_system.clone(), tokio_actor_system.clone());
    actor1_ref.send(Ping(), None);
    actor2_ref.join();
    log::info!("Joined Actor2");
    actor1_ref.join();
    log::info!("Joined Actor1");
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

    use bftgrid_core::actor::ActorRef;
    use bftgrid_sim::{NodeDescriptor, Simulation};

    use crate::{build_system, utils, Ping, System};

    #[test]
    fn simulation() {
        utils::setup_logging(true);
        let mut topology = HashMap::new();
        topology.insert("node".into(), NodeDescriptor::default());
        let start = Instant::now();
        let simulation = Simulation::new(topology, start, start.add(Duration::from_secs(100)));
        let System { mut actor1_ref, .. } = build_system(simulation.clone(), simulation.clone());
        actor1_ref.send(Ping(), None);
        let history = simulation.run();
        log::info!("{:?}", history);
    }
}
