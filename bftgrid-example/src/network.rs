mod utils;

use std::any::Any;

use bftgrid_core::actor::{
    erased::{DynActorRef, DynActorSystemHandle, DynP2PNetworkClient},
    ActorControl, ActorMsg, ActorRef, ActorSystemHandle, AnActorMsg, Joinable, MessageNotSupported,
    P2PNetworkClient, TypedMsgHandler, UntypedMsgHandler,
};

use bftgrid_mt::{
    thread::ThreadActorSystemHandle,
    tokio::{TokioActorSystemHandle, TokioP2PNetworkClient, TokioP2PNetworkServer},
    AsyncRuntime,
};
use tokio::net::UdpSocket;

#[derive(Clone, Debug)]
struct Ping();
impl ActorMsg for Ping {}

struct Actor1 {
    self_ref: DynActorRef<AnActorMsg>,
    node_id: String,
    actor_system: DynActorSystemHandle,
    network_out: DynP2PNetworkClient,
    ping_count: u8,
    spawn_count: u8,
}

impl Actor1 {
    fn new(
        self_ref: DynActorRef<AnActorMsg>,
        node_id: impl Into<String>,
        actor_system: DynActorSystemHandle,
        network_out: DynP2PNetworkClient,
    ) -> Actor1 {
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

impl TypedMsgHandler<AnActorMsg> for Actor1 {
    fn receive(&mut self, msg: AnActorMsg) -> Option<ActorControl> {
        if (msg as Box<dyn Any>).downcast::<Ping>().is_ok() {
        } else {
            panic!("Not a Ping")
        }
        let ret = match self.ping_count {
            0 => {
                log::info!("Actor1 received first ping, sending ping to Actor2 over the network");
                let mut out = self.network_out.clone();
                let _ = out.attempt_send(
                    Box::new(Ping {}),
                    Box::new(|_msg| Ok(Vec::new())),
                    "localhost:5002".into(),
                );
                log::info!("Actor1 sent ping to Actor2 over the network");
                None
            }
            1 => {
                log::info!("Actor1 received second ping, self-pinging after async work");
                self.self_ref.spawn_async_send(
                    Box::pin(async {
                        log::info!("Actor1 doing async work");
                        Box::new(Ping()) as AnActorMsg
                    }),
                    None,
                );
                None
            }
            _ => {
                log::info!("Actor1 received third ping");
                if self.spawn_count < 1 {
                    log::info!("Actor1 spawning");
                    let mut new_ref = self.actor_system.create(
                        self.node_id.clone(),
                        self.spawn_count.to_string(),
                        false,
                    );
                    log::info!("Actor1 setting handler");
                    new_ref.set_handler(Box::new(Actor1 {
                        self_ref: self.self_ref.clone(),
                        node_id: self.node_id.clone(),
                        actor_system: self.actor_system.clone(),
                        network_out: self.network_out.clone(),
                        ping_count: 3,
                        spawn_count: self.spawn_count + 1,
                    }));
                    log::info!("Actor1 sending to spawned actor");
                    new_ref.send(Box::new(Ping()), None);
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
struct Actor2<P2PNetworkClientT>
where
    P2PNetworkClientT: P2PNetworkClient,
{
    network_out: P2PNetworkClientT,
}

impl<P2PNetworkClientT> Actor2<P2PNetworkClientT>
where
    P2PNetworkClientT: P2PNetworkClient,
{
    fn new(network_out: P2PNetworkClientT) -> Self {
        Actor2 { network_out }
    }
}

impl<P2PNetworkClientT> TypedMsgHandler<AnActorMsg> for Actor2<P2PNetworkClientT>
where
    P2PNetworkClientT: P2PNetworkClient + Clone + Send + std::fmt::Debug + 'static,
{
    fn receive(&mut self, msg: AnActorMsg) -> Option<ActorControl> {
        if (msg.clone() as Box<dyn Any>).downcast::<Ping>().is_ok() {
        } else {
            panic!("Not a Ping")
        }
        log::info!("Actor2 received ping over the network, replying with a ping over the network");
        let mut out = self.network_out.clone();
        let _ = out.attempt_send(msg, |_msg| Ok(Vec::new()), "localhost:5001");
        log::info!("Actor2 sent ping reply, exiting");
        Some(ActorControl::Exit())
    }
}

#[derive(Debug)]
struct NodeP2PNetworkInputHandler<ActorRefT>
where
    ActorRefT: ActorRef<AnActorMsg>,
{
    actor_ref: ActorRefT,
}

impl<ActorRefT> UntypedMsgHandler for NodeP2PNetworkInputHandler<ActorRefT>
where
    ActorRefT: ActorRef<AnActorMsg>,
{
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported> {
        self.actor_ref.send(message, None);
        Result::Ok(None)
    }
}

// Components that need a Tokio runtime will reuse the one from the async context, if any,
//  otherwise they will create a new one.
#[tokio::main]
async fn main() {
    utils::setup_logging(false);
    let async_runtime = AsyncRuntime::new("main", None);
    let network1 = TokioP2PNetworkClient::new("network1", vec!["localhost:5002"], None);
    let network2 = TokioP2PNetworkClient::new("network2", vec!["localhost:5001"], None);
    let mut tokio_actor_system = TokioActorSystemHandle::new_actor_system("tokio-as", None, false);
    let mut thread_actor_system =
        ThreadActorSystemHandle::new_actor_system("thread-as", None, false);
    let mut actor1_ref: bftgrid_mt::tokio::TokioActorRef<AnActorMsg> =
        tokio_actor_system.create("node1", "actor1", false);
    let actor1_ref_copy = actor1_ref.clone();
    actor1_ref.set_handler(Box::new(Actor1::new(
        Box::new(actor1_ref_copy),
        "node1",
        Box::new(tokio_actor_system.clone()),
        Box::new(network1.clone()),
    )));
    let mut actor2_ref: bftgrid_mt::thread::ThreadActorRef<AnActorMsg> =
        thread_actor_system.create("node2", "actor2", false);
    actor2_ref.set_handler(Box::new(Actor2::new(network2.clone())));
    let node1 = TokioP2PNetworkServer::new(
        "node1",
        async_runtime.block_on_async(async {
            UdpSocket::bind("localhost:5001")
                .await
                .expect("Cannot bind")
        }),
        None,
    );
    let node1_p2p_network_input_handler = NodeP2PNetworkInputHandler {
        actor_ref: actor1_ref.clone(),
    };
    drop(
        node1
            .start(node1_p2p_network_input_handler, |_buf| Ok(Ping {}), 0)
            .unwrap(),
    );
    log::info!("Started node1");
    let node2 = TokioP2PNetworkServer::new(
        "node2",
        async_runtime.block_on_async(async {
            UdpSocket::bind("localhost:5002")
                .await
                .expect("Cannot bind")
        }),
        None,
    );
    let node2_p2p_network_input_handler = NodeP2PNetworkInputHandler {
        actor_ref: actor2_ref.clone(),
    };
    drop(
        node2
            .start(node2_p2p_network_input_handler, |_buf| Ok(Ping {}), 0)
            .unwrap(),
    );
    log::info!("Started node2");
    actor1_ref.send(Box::new(Ping()), None);
    log::info!("Sent startup ping to actor1; joining actors, actor systems and exiting");
    actor2_ref.join();
    log::info!("Joined Actor2");
    actor1_ref.join();
    log::info!("Joined Actor1");
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

    use crate::utils;
    use bftgrid_core::actor::ActorSystemHandle;
    use bftgrid_sim::{NodeDescriptor, Simulation};

    use crate::{Actor1, Actor2, ActorRef, Ping};

    #[test]
    fn simulation() {
        utils::setup_logging(true);
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
        let simulation = Simulation::new(topology, start, start.add(Duration::from_secs(100)));
        let mut actor1_ref = simulation.create("localhost:5001", "actor1", false);
        let actor1_ref_copy = actor1_ref.clone();
        actor1_ref.set_handler(Box::new(Actor1::new(
            Box::new(actor1_ref_copy),
            "localhost:5001",
            Box::new(simulation.clone()),
            Box::new(simulation.clone()),
        )));
        let mut actor2_ref = simulation.create("localhost:5002", "actor2", false);
        actor2_ref.set_handler(Box::new(Actor2::new(simulation.clone())));
        actor1_ref.send(Box::new(Ping()), None);
        let history = simulation.run();
        log::info!("{:?}", history);
    }
}
