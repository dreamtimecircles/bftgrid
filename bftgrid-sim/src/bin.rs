use std::{
    collections::HashMap,
    ops::Add,
    time::{Duration, Instant},
};

use bftgrid_core::{ActorControl, ActorRef, ActorSystem, TypedHandler};
use bftgrid_sim::{Node, Simulation};

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
    topology.insert("node".into(), Node::new());
    let mut simulation = Simulation::new(topology, start, end);
    let mut test1 = simulation.create::<(), TestHandler>("node".into(), "test1".into());
    let mut test2 = simulation.create::<(), TestHandler>("node".into(), "test2".into());
    simulation.set_handler(&mut test1, TestHandler {});
    simulation.set_handler(&mut test2, TestHandler {});
    test1.send((), None);
    test2.send((), None);
    simulation.run();
}
