use std::sync::{Arc, Mutex};

use bftgrid_core::{
    ActorControl, ActorMsg, ActorRef, ActorSystem, Joinable, MessageNotSupported, P2PNetwork,
    TypedHandler, UntypedHandler,
};

use bftgrid_mt::{
    thread::ThreadActorSystem,
    tokio::{TokioActorSystem, TokioNetworkNode, TokioP2PNetwork},
};
use tokio::net::UdpSocket;

#[derive(Clone, Debug)]
struct Ping();
impl ActorMsg for Ping {}

#[derive(Debug)]
struct Actor1<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + 'static,
    P2PNetworkT: P2PNetwork,
{
    self_ref: Box<dyn ActorRef<Ping, Actor1<ActorSystemT, P2PNetworkT>>>,
    node_id: String,
    actor_system: ActorSystemT,
    network_out: P2PNetworkT,
    ping_count: u8,
    spawn_count: u8,
}

impl<ActorSystemT, P2PNetworkT> Actor1<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem,
    P2PNetworkT: P2PNetwork,
{
    fn new(
        self_ref: Box<dyn ActorRef<Ping, Actor1<ActorSystemT, P2PNetworkT>>>,
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

impl<ActorSystemT, P2PNetworkT> TypedHandler<'_> for Actor1<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + Send + std::fmt::Debug + 'static,
    P2PNetworkT: P2PNetwork + Send + std::fmt::Debug + 'static,
{
    type MsgT = Ping;

    fn receive(&mut self, _msg: Ping) -> Option<ActorControl> {
        let ret = match self.ping_count {
            0 => {
                println!("Actor1 received first ping, sending ping to Actor2 over the network");
                let mut out = self.network_out.clone();
                out.send::<0, _, _>(Ping {}, &|_msg, _bytes| Ok(0), "localhost:5002");
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
                    let mut new_ref = self
                        .actor_system
                        .create::<Ping, Actor1<ActorSystemT, P2PNetworkT>>(
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
                            network_out: self.network_out.clone(),
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

#[derive(Debug)]
struct Actor2<P2PNetworkT>
where
    P2PNetworkT: P2PNetwork,
{
    network_out: P2PNetworkT,
}

impl<P2PNetworkT> Actor2<P2PNetworkT>
where
    P2PNetworkT: P2PNetwork,
{
    fn new(network_out: P2PNetworkT) -> Self {
        Actor2 { network_out }
    }
}

impl<P2PNetworkT> TypedHandler<'_> for Actor2<P2PNetworkT>
where
    P2PNetworkT: P2PNetwork + Send + std::fmt::Debug + 'static,
{
    type MsgT = Ping;

    fn receive(&mut self, msg: Self::MsgT) -> Option<ActorControl> {
        println!("Actor2 received ping over the network, replying with a ping over the network");
        let mut out = self.network_out.clone();
        out.send::<0, _, _>(msg, &|_msg, _bytes| Ok(0), "localhost:5001");
        println!("Actor2 sent ping, exiting");
        Some(ActorControl::Exit())
    }
}

#[derive(Debug)]
struct Node1P2pNetworkInputHandler<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + 'static,
    P2PNetworkT: P2PNetwork,
{
    actor1_ref: Box<dyn ActorRef<Ping, Actor1<ActorSystemT, P2PNetworkT>>>,
}

impl<'msg, ActorSystemT, P2PNetworkT> UntypedHandler<'msg>
    for Node1P2pNetworkInputHandler<ActorSystemT, P2PNetworkT>
where
    ActorSystemT: ActorSystem + std::fmt::Debug + Send + 'static,
    P2PNetworkT: P2PNetwork + std::fmt::Debug + Send + 'static,
{
    fn receive_untyped(
        &mut self,
        message: Box<dyn ActorMsg + 'msg>,
    ) -> Result<Option<ActorControl>, bftgrid_core::MessageNotSupported> {
        match message.downcast::<Ping>() {
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
    P2PNetworkT: P2PNetwork,
{
    actor2_ref: Box<dyn ActorRef<Ping, Actor2<P2PNetworkT>>>,
}

impl<'msg, P2PNetworkT> UntypedHandler<'msg> for Node2P2pNetworkInputHandler<P2PNetworkT>
where
    P2PNetworkT: P2PNetwork + std::fmt::Debug + Send + 'static,
{
    fn receive_untyped(
        &mut self,
        message: Box<dyn ActorMsg + 'msg>,
    ) -> Result<Option<ActorControl>, bftgrid_core::MessageNotSupported> {
        match message.downcast::<Ping>() {
            Ok(typed_message) => {
                self.actor2_ref.send(*typed_message, None);
                Result::Ok(None)
            }
            Err(_) => Result::Err(MessageNotSupported()),
        }
    }
}

#[tokio::main]
async fn main() {
    let network1 = TokioP2PNetwork::new(vec!["localhost:5002"]).await;
    let mut network1_handle = network1.clone();
    let network2 = TokioP2PNetwork::new(vec!["localhost:5001"]).await;
    let mut network2_handle = network2.clone();
    let mut tokio_actor_system = TokioActorSystem::new();
    let mut thread_actor_system = ThreadActorSystem::new();
    let mut actor1_ref = tokio_actor_system.create("node1", "actor1");
    let actor1_ref_copy = actor1_ref.new_ref();
    tokio_actor_system.set_handler(
        &mut actor1_ref,
        Actor1::new(
            actor1_ref_copy,
            "node1",
            tokio_actor_system.clone(),
            network1,
        ),
    );
    let mut actor2_ref = thread_actor_system.create("node2", "actor2");
    thread_actor_system.set_handler(&mut actor2_ref, Actor2::new(network2));
    let node1 = TokioNetworkNode::new(
        Arc::new(Mutex::new(Box::new(Node1P2pNetworkInputHandler {
            actor1_ref: actor1_ref.new_ref(),
        }))),
        UdpSocket::bind("localhost:5001")
            .await
            .expect("Cannot bind"),
    )
    .unwrap();
    node1.start::<0, _, _>(|_buf| Ok(Ping {}));
    let node2 = TokioNetworkNode::new(
        Arc::new(Mutex::new(Box::new(Node2P2pNetworkInputHandler {
            actor2_ref: actor2_ref.new_ref(),
        }))),
        UdpSocket::bind("localhost:5002")
            .await
            .expect("Cannot bind"),
    )
    .unwrap();
    node2.start::<0, _, _>(|_buf| Ok(Ping {}));
    network1_handle.connect().await;
    network2_handle.connect().await;
    actor1_ref.send(Ping(), None);
    actor2_ref.join();
    println!("Joined Actor2");
    actor1_ref.join();
    println!("Joined Actor1");
    tokio_actor_system.join().await;
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        ops::Add,
        time::{Duration, Instant},
    };

    use bftgrid_core::ActorSystem;
    use bftgrid_sim::{NodeDescriptor, Simulation};

    use crate::{Actor1, Actor2, ActorRef, Ping};

    #[test]
    fn simulation() {
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
        println!("{:?}", history);
    }
}
