use std::{
    ops::Add,
    time::{Duration, Instant},
};

use bft_grid_core::{SingleThreadedActorSystem, TypedMessageHandler};
use bft_grid_sim::Simulation;

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
    let mut simulation = Simulation::new(start, end);
    let handler1 = TestHandler {};
    let handler2 = TestHandler {};
    let test1 = simulation.spawn_actor("test1".into(), handler1);
    let test2 = simulation.spawn_actor("test2".into(), handler2);
    test1.async_send(());
    test2.async_send(());
    simulation.run();
}
