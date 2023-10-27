use std::{
    collections::HashMap,
    ops::Add,
    time::{Duration, Instant},
};

use bft_grid_core::{ActorControl, SimulatedActorSystem, TypedHandler};
use bft_grid_sim::{Node, Simulation};

struct TestHandler {}

impl TypedHandler<'_> for TestHandler {
    type MsgT = ();

    fn receive(&mut self, _: ()) -> Option<ActorControl> {
        println!("Received");
        None
    }
}

fn main() {
    let start = Instant::now();
    let end = start.add(Duration::from_secs(60));
    let mut topology = HashMap::<String, Node>::new();
    topology.insert(
        "node".into(),
        Node::new(Box::new(TestHandler {}), Box::new(TestHandler {})),
    );
    let mut simulation = Simulation::new(topology, start, end);
    let handler1 = TestHandler {};
    let handler2 = TestHandler {};
    let mut test1 = simulation.spawn_actor("node".into(), "test1".into(), handler1);
    let mut test2 = simulation.spawn_actor("node".into(), "test2".into(), handler2);
    test1.send((), None);
    test2.send((), None);
    simulation.run();
}
