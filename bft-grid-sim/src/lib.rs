use std::{
    any::Any,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    time::Instant,
};

use bft_grid_core::{ActorRef, ActorSystem, TypedMessageHandler, UntypedMessageHandler};

pub struct SimulationClock {
    current_instant: Instant,
}

impl SimulationClock {
    fn now(&self) -> Instant {
        self.current_instant
    }
}

struct EventAtInstant<'actor_name> {
    target_actor_name: &'actor_name str,
    event: Box<dyn Any>,
    instant: Instant,
}

impl<'actor_name> PartialEq for EventAtInstant<'actor_name> {
    fn eq(&self, other: &Self) -> bool {
        self.instant == other.instant
    }
}

impl<'actor_name> Eq for EventAtInstant<'actor_name> {}

impl<'actor_name> PartialOrd for EventAtInstant<'actor_name> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.instant.partial_cmp(&other.instant) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.instant.partial_cmp(&other.instant)
    }
}

impl<'actor_name> Ord for EventAtInstant<'actor_name> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.instant.cmp(&other.instant)
    }
}

pub struct Simulation<'actor_name, 'msg> {
    events_queue: BinaryHeap<EventAtInstant<'actor_name>>,
    clock: SimulationClock,
    end_instant: Instant,
    handlers: HashMap<&'actor_name str, Box<dyn UntypedMessageHandler<'msg>>>,
}

impl<'actor_name, 'msg> Simulation<'actor_name, 'static> {
    fn new(start_instant: Instant, end_instant: Instant) -> Simulation<'actor_name, 'static> {
        Simulation {
            events_queue: BinaryHeap::new(),
            clock: SimulationClock {
                current_instant: start_instant,
            },
            end_instant,
            handlers: HashMap::new(),
        }
    }

    fn run(&mut self) {
        while !self.events_queue.is_empty() {
            let e = self.events_queue.pop().unwrap();

            match self.handlers.get_mut(&e.target_actor_name) {
                Some(handler) => {
                    match handler.receive_untyped(e.event) {
                        Ok(_) => (),
                        Err(_) => (), // TODO log
                    }
                }
                None => todo!(),
            }
        }
    }
}

pub struct SimulationActor<'actor_name, M> {
    actor_name: &'actor_name str,
    events_queue: &'actor_name mut BinaryHeap<EventAtInstant<'actor_name>>,
    clock: &'actor_name SimulationClock,
    message_type: PhantomData<M>,
}

impl<'actor_name, M: 'static> ActorRef<M> for SimulationActor<'actor_name, M> {
    fn async_send(&mut self, message: M) -> () {
        self.events_queue.push(EventAtInstant {
            target_actor_name: self.actor_name,
            event: Box::new(message),
            instant: self.clock.now(),
        });
    }
}

pub struct SimulationActorSystem<'actor_name, 'msg> {
    pub simulation: Simulation<'actor_name, 'msg>,
}

impl<'actor_name, 'this: 'actor_name> ActorSystem<'this, 'actor_name>
    for SimulationActorSystem<'actor_name, 'static>
{
    fn spawn_actor<
        Msg: 'static + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &'this mut self,
        name: &'actor_name str,
        handler: MH,
    ) -> Box<dyn ActorRef<Msg> + 'this> {
        self.simulation.handlers.insert(name, Box::new(handler));
        Box::new(SimulationActor {
            actor_name: name,
            events_queue: &mut self.simulation.events_queue,
            clock: &self.simulation.clock,
            message_type: PhantomData {},
        })
    }
}
