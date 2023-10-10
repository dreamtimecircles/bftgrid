use std::{
    any::Any,
    cell::RefCell,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    rc::Rc,
    time::Instant,
};

use bft_grid_core::{
    SingleThreadedActorRef, SingleThreadedActorSystem, TypedMessageHandler, UntypedMessageHandler,
};

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
        other.instant.partial_cmp(&self.instant) // Reverse order: earlier is bigger
    }
}

impl Ord for EventAtInstant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.instant.cmp(&self.instant) // Reverse order: earlier is bigger
    }
}

pub struct Simulation {
    events_queue: Rc<RefCell<BinaryHeap<EventAtInstant>>>,
    clock: Rc<RefCell<SimulationClock>>,
    end_instant: Instant,
    handlers: HashMap<Rc<String>, Box<dyn UntypedMessageHandler<'static>>>,
}

struct SimulationEnd();

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
            event: Box::new(SimulationEnd()),
            instant: self.end_instant,
        });

        while !self.events_queue.borrow().is_empty() {
            let e = self.events_queue.borrow_mut().pop().unwrap();
            self.clock.borrow_mut().current_instant = e.instant;

            if e.event.downcast_ref::<SimulationEnd>().is_some() {
                return;
            }

            match self.handlers.get_mut(&e.target_actor_name) {
                Some(handler) => {
                    handler
                        .receive_untyped(e.event)
                        .expect("Found event targeting the wrong actor");
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

impl<M> SingleThreadedActorRef<M> for SimulationActor<M>
where
    M: 'static,
{
    fn async_send(&self, message: M) {
        self.events_queue.borrow_mut().push(EventAtInstant {
            target_actor_name: self.actor_name.clone(),
            event: Box::new(message),
            instant: self.clock.borrow().current_instant,
        });
    }
}

impl SingleThreadedActorSystem for Simulation {
    fn spawn_actor<Msg, MH>(
        &mut self,
        name: String,
        handler: MH,
    ) -> Box<dyn SingleThreadedActorRef<Msg>>
    where
        Msg: 'static,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg>,
    {
        let shared_actor_name = Rc::new(name);
        if !self
            .handlers
            .insert(shared_actor_name.clone(), Box::new(handler))
            .is_none()
        {
            panic!("An actor with such a name already exist");
        }
        Box::new(SimulationActor {
            actor_name: shared_actor_name,
            events_queue: self.events_queue.clone(),
            clock: self.clock.clone(),
            message_type: PhantomData {},
        })
    }
}
