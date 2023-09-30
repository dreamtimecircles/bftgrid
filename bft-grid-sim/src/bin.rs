use std::{time::{Instant, Duration}, ops::Add, borrow::Cow};

use bft_grid_core::{ActorSystem, TypedMessageHandler};
use bft_grid_sim::{Simulation, SimulationActorSystem};

struct TestHandler {}

impl TypedMessageHandler<'_> for TestHandler {
    type Msg = ();

    fn receive(&mut self, _: ()) -> () {
        println!("Received")
    }
}

fn main() {
    let start = Instant::now();
    let end = start.add(Duration::from_secs(60));
    let mut simulation = Simulation::new(start, end);
    let mut actor_system = SimulationActorSystem{simulation: &mut simulation};
    let handler1 = TestHandler{};
    let handler2 = TestHandler{};
    let mut test1 = actor_system.spawn_actor(Cow::Borrowed("test1"), handler1);
    let mut test2 = actor_system.spawn_actor(Cow::Borrowed("test2"), handler2);
    test1.async_send(());
    test2.async_send(());
    simulation.run();
}
