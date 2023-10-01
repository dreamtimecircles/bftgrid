use std::{
    any::Any,
    cell::RefCell,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    rc::Rc,
    time::Instant,
};

use bft_grid_core::{ActorRef, ActorSystem, TypedMessageHandler, UntypedMessageHandler};

pub struct SimulationClock {
    pub current_instant: Instant,
}

struct EventAtInstant {
    target_actor_name: Rc<String>,
    event: Box<dyn Any>,
    instant: Instant,
}

impl PartialEq for EventAtInstant {
    fn eq(&self, other: &Self) -> bool {
        self.instant == other.instant
    }
}

impl Eq for EventAtInstant {}

impl PartialOrd for EventAtInstant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.instant.partial_cmp(&other.instant) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.instant.partial_cmp(&other.instant)
    }
}

impl Ord for EventAtInstant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.instant.cmp(&other.instant)
    }
}

pub struct Simulation {
    events_queue: Rc<RefCell<BinaryHeap<EventAtInstant>>>,
    clock: Rc<RefCell<SimulationClock>>,
    end_instant: Instant,
    handlers: HashMap<Rc<String>, Box<dyn UntypedMessageHandler<'static>>>,
}

impl Simulation {
    pub fn new(start_instant: Instant, end_instant: Instant) -> Simulation {
        Simulation {
            events_queue: Rc::new(RefCell::new(BinaryHeap::new())),
            clock: Rc::new(RefCell::new(SimulationClock {
                current_instant: start_instant,
            })),
            end_instant,
            handlers: HashMap::new(),
        }
    }

    pub fn run(mut self) {
        self.events_queue.borrow_mut().push(EventAtInstant {
            target_actor_name: Rc::new("".to_string()),
            event: Box::new(()),
            instant: self.end_instant,
        });

        while !self.events_queue.borrow().is_empty() {
            let e = self.events_queue.borrow_mut().pop().unwrap();
            self.clock.borrow_mut().current_instant = e.instant;

            if e.event.downcast_ref::<()>().is_some() {
                return;
            }

            match self.handlers.get_mut(&e.target_actor_name) {
                Some(handler) => {
                    handler.receive_untyped(e.event).expect("Found event targeting the wrong actor");
                }
                None => todo!(),
            }
        }
    }
}

pub struct SimulationActor<M> {
    actor_name: Rc<String>,
    events_queue: Rc<RefCell<BinaryHeap<EventAtInstant>>>,
    clock: Rc<RefCell<SimulationClock>>,
    message_type: PhantomData<M>,
}

impl<M: 'static> ActorRef<M> for SimulationActor<M> {
    fn async_send(&self, message: M) {
        self.events_queue.borrow_mut().push(EventAtInstant {
            target_actor_name: self.actor_name.clone(),
            event: Box::new(message),
            instant: self.clock.borrow().current_instant,
        });
    }
}

impl ActorSystem for Simulation {
    fn spawn_actor<
        Msg: 'static + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &mut self,
        name: String,
        handler: MH,
    ) -> Box<dyn ActorRef<Msg>> {
        let shared_actor_name = Rc::new(name);
        self.handlers
            .insert(shared_actor_name.clone(), Box::new(handler)).expect("An actor with such a name already exist");
        Box::new(SimulationActor {
            actor_name: shared_actor_name,
            events_queue: self.events_queue.clone(),
            clock: self.clock.clone(),
            message_type: PhantomData {},
        })
    }
}
