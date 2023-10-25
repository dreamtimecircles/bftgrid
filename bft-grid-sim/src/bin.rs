use std::{
    collections::HashMap,
    ops::Add,
    time::{Duration, Instant},
};

use bft_grid_core::{SingleThreadedActorSystem, TypedMessageHandler};
use bft_grid_sim::{Node, Simulation};

struct TestHandler {}

impl TypedMessageHandler<'_> for TestHandler {
    type Msg = ();

    fn receive(&mut self, _: ()) {
        println!("Received")
    }
}

fn main() {
    let start = Instant::now();
    let end = start.add(Duration::from_secs(60));
    let mut topology = HashMap::<String, Node>::new();
    topology.insert(
        "node".into(),
        Node::new(
            Box::new(TestHandler {}),
            Box::new(TestHandler {}),
            HashMap::new(),
        ),
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
