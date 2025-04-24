use std::any::Any;

use bftgrid_core::{
    ActorControl, ActorMsg, ActorRef, ActorSystem, AnActorMsg, AnActorRef, Joinable,
    MessageNotSupported, P2PNetworkClient, TypedHandler, UntypedHandler,
};

use bftgrid_example::setup_logging;
use bftgrid_mt::{
    get_async_runtime,
    thread::ThreadActorSystem,
    tokio::{TokioActorSystem, TokioP2PNetworkClient, TokioP2PNetworkServer},
};
use tokio::net::UdpSocket;

#[derive(Clone, Debug)]
struct Ping();
impl ActorMsg for Ping {}

#[derive(Debug)]
struct Actor1<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + 'static,
    P2PNetworkT: P2PNetworkClient,
{
    self_ref: AnActorRef<Ping, Actor1<ActorSystemT, P2PNetworkT>>,
    node_id: String,
    actor_system: ActorSystemT,
    network_out: P2PNetworkT,
    ping_count: u8,
    spawn_count: u8,
}

impl<ActorSystemT, P2PNetworkT> Actor1<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem,
    P2PNetworkT: P2PNetworkClient,
{
    fn new(
        self_ref: AnActorRef<Ping, Actor1<ActorSystemT, P2PNetworkT>>,
        node_id: impl Into<String>,
        actor_system: ActorSystemT,
        network_out: P2PNetworkT,
    ) -> Actor1<ActorSystemT, P2PNetworkT> {
        Actor1 {
            self_ref,
            node_id: node_id.into(),
            actor_system,
            network_out,
            ping_count: 0,
            spawn_count: 0,
        }
    }
}

impl<ActorSystemT, P2PNetworkT> TypedHandler for Actor1<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + Send + std::fmt::Debug + 'static,
    P2PNetworkT: P2PNetworkClient + Send + std::fmt::Debug + 'static,
{
    type MsgT = Ping;

    fn receive(&mut self, _msg: Ping) -> Option<ActorControl> {
        let ret = match self.ping_count {
            0 => {
                log::info!("Actor1 received first ping, sending ping to Actor2 over the network");
                let mut out = self.network_out.clone();
                let _ = out.attempt_send(Ping {}, &|_msg| Ok(Vec::new()), "localhost:5002");
                log::info!("Actor1 sent ping to Actor2 over the network");
                None
            }
            1 => {
                log::info!("Actor1 received second ping, self-pinging after async work");
                self.actor_system.spawn_async_send(
                    async {
                        log::info!("Actor1 doing async work");
                    },
                    |_| Ping {},
                    self.self_ref.new_ref(),
                    None,
                );
                None
            }
            _ => {
                log::info!("Actor1 received third ping");
                if self.spawn_count < 1 {
                    log::info!("Actor1 spawning");
                    let mut new_ref = self
                        .actor_system
                        .create::<Ping, Actor1<ActorSystemT, P2PNetworkT>>(
                            self.node_id.clone(),
                            self.spawn_count.to_string(),
                        );
                    log::info!("Actor1 setting handler");
                    self.actor_system.set_handler(
                        &mut new_ref,
                        Actor1 {
                            self_ref: self.self_ref.new_ref(),
                            node_id: self.node_id.clone(),
                            actor_system: self.actor_system.clone(),
                            network_out: self.network_out.clone(),
                            ping_count: 3,
                            spawn_count: self.spawn_count + 1,
                        },
                    );
                    log::info!("Actor1 sending to spawned actor");
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

#[derive(Debug)]
struct Actor2<P2PNetworkT>
where
    P2PNetworkT: P2PNetworkClient,
{
    network_out: P2PNetworkT,
}

impl<P2PNetworkT> Actor2<P2PNetworkT>
where
    P2PNetworkT: P2PNetworkClient,
{
    fn new(network_out: P2PNetworkT) -> Self {
        Actor2 { network_out }
    }
}

impl<P2PNetworkT> TypedHandler for Actor2<P2PNetworkT>
where
    P2PNetworkT: P2PNetworkClient + Send + std::fmt::Debug + 'static,
{
    type MsgT = Ping;

    fn receive(&mut self, msg: Self::MsgT) -> Option<ActorControl> {
        log::info!("Actor2 received ping over the network, replying with a ping over the network");
        let mut out = self.network_out.clone();
        let _ = out.attempt_send(msg, &|_msg| Ok(Vec::new()), "localhost:5001");
        log::info!("Actor2 sent ping reply, exiting");
        Some(ActorControl::Exit())
    }
}

#[derive(Debug)]
struct Node1P2pNetworkInputHandler<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + 'static,
    P2PNetworkT: P2PNetworkClient,
{
    actor1_ref: AnActorRef<Ping, Actor1<ActorSystemT, P2PNetworkT>>,
}

impl<ActorSystemT, P2PNetworkT> UntypedHandler
    for Node1P2pNetworkInputHandler<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + std::fmt::Debug + Send + 'static,
    P2PNetworkT: P2PNetworkClient + std::fmt::Debug + Send + 'static,
{
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, bftgrid_core::MessageNotSupported> {
        match (message as Box<dyn Any>).downcast::<Ping>() {
            Ok(typed_message) => {
                self.actor1_ref.send(*typed_message, None);
                Result::Ok(None)
            }
            Err(_) => Result::Err(MessageNotSupported()),
        }
    }
}

#[derive(Debug)]
struct Node2P2pNetworkInputHandler<P2PNetworkT>
where
    P2PNetworkT: P2PNetworkClient,
{
    actor2_ref: AnActorRef<Ping, Actor2<P2PNetworkT>>,
}

impl<P2PNetworkT> UntypedHandler for Node2P2pNetworkInputHandler<P2PNetworkT>
where
    P2PNetworkT: P2PNetworkClient + std::fmt::Debug + Send + 'static,
{
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, bftgrid_core::MessageNotSupported> {
        match (message as Box<dyn Any>).downcast::<Ping>() {
            Ok(typed_message) => {
                self.actor2_ref.send(*typed_message, None);
                Result::Ok(None)
            }
            Err(_) => Result::Err(MessageNotSupported()),
        }
    }
}

// Components that need a Tokio runtime will reuse the one from the async context, if any,
//  otherwise they will create a new one.
#[tokio::main]
async fn main() {
    setup_logging(false);
    let async_runtime = get_async_runtime("main");
    let network1 = TokioP2PNetworkClient::new("network1", vec!["localhost:5002"]);
    let network2 = TokioP2PNetworkClient::new("network2", vec!["localhost:5001"]);
    let mut tokio_actor_system = TokioActorSystem::new("tokio-as");
    let mut thread_actor_system = ThreadActorSystem::new("thread-as");
    let mut actor1_ref = tokio_actor_system.create("node1", "actor1");
    let actor1_ref_copy = actor1_ref.new_ref();
    tokio_actor_system.set_handler(
        &mut actor1_ref,
        Actor1::new(
            actor1_ref_copy,
            "node1",
            tokio_actor_system.clone(),
            network1.clone(),
        ),
    );
    let mut actor2_ref = thread_actor_system.create("node2", "actor2");
    thread_actor_system.set_handler(&mut actor2_ref, Actor2::new(network2.clone()));
    let node1 = TokioP2PNetworkServer::new(
        "node1",
        async_runtime.await_async(async {
            UdpSocket::bind("localhost:5001")
                .await
                .expect("Cannot bind")
        }),
    );
    node1
        .start(
            Node1P2pNetworkInputHandler {
                actor1_ref: actor1_ref.new_ref(),
            },
            |_buf| Ok(Ping {}),
            0,
        )
        .unwrap();
    log::info!("Started node1");
    let node2 = TokioP2PNetworkServer::new(
        "node2",
        async_runtime.await_async(async {
            UdpSocket::bind("localhost:5002")
                .await
                .expect("Cannot bind")
        }),
    );
    node2
        .start(
            Node2P2pNetworkInputHandler {
                actor2_ref: actor2_ref.new_ref(),
            },
            |_buf| Ok(Ping {}),
            0,
        )
        .unwrap();
    log::info!("Started node2");
    actor1_ref.send(Ping(), None);
    log::info!(
        "Sent startup ping to actor1; joining actors, stopping servers and joining actor systems"
    );
    actor2_ref.join();
    log::info!("Joined Actor2");
    actor1_ref.join();
    log::info!("Joined Actor1");
    node1.stop();
    log::info!("Stopped node1");
    node2.stop();
    log::info!("Stopped node2");
    tokio_actor_system.join();
    log::info!("Joined Tokio actor system");
    thread_actor_system.join();
    log::info!("Joined Thread actor system");
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        ops::Add,
        time::{Duration, Instant},
    };

    use bftgrid_core::ActorSystem;
    use bftgrid_example::setup_logging;
    use bftgrid_sim::{NodeDescriptor, Simulation};

    use crate::{Actor1, Actor2, ActorRef, Ping};

    #[test]
    fn simulation() {
        setup_logging(true);
        let mut topology = HashMap::new();
        topology.insert(
            "localhost:5001".into(),
            NodeDescriptor::new(None::<&str>, Some("actor1")),
        );
        topology.insert(
            "localhost:5002".into(),
            NodeDescriptor::new(None::<&str>, Some("actor2")),
        );
        let start = Instant::now();
        let mut simulation = Simulation::new(topology, start, start.add(Duration::from_secs(100)));
        let mut actor1_ref = simulation.create("localhost:5001", "actor1");
        let actor1_ref_copy = actor1_ref.new_ref();
        simulation.set_handler(
            &mut actor1_ref,
            Actor1::new(
                actor1_ref_copy,
                "localhost:5001",
                simulation.clone(),
                simulation.clone(),
            ),
        );
        let mut actor2_ref = simulation.create("localhost:5002", "actor2");
        simulation.set_handler(&mut actor2_ref, Actor2::new(simulation.clone()));
        actor1_ref.send(Ping(), None);
        let history = simulation.run();
        log::info!("{:?}", history);
    }
}
