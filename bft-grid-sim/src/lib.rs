use std::{
    any::Any,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    sync::Arc,
    sync::RwLock,
    time::Instant,
};

use bft_grid_core::{ActorRef, ActorSystem, TypedMessageHandler, UntypedMessageHandler};

pub struct SimulationClock {
    pub current_instant: Instant,
}

struct EventAtInstant {
    target_actor_name: Arc<String>,
    event: Box<dyn Any + Send + Sync>,
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
    events_queue: Arc<RwLock<BinaryHeap<EventAtInstant>>>,
    clock: Arc<RwLock<SimulationClock>>,
    end_instant: Instant,
    handlers: HashMap<Arc<String>, Box<dyn UntypedMessageHandler<'static>>>,
}

struct SimulationEnd();

impl Simulation {
    pub fn new(start_instant: Instant, end_instant: Instant) -> Simulation {
        Simulation {
            events_queue: Arc::new(RwLock::new(BinaryHeap::new())),
            clock: Arc::new(RwLock::new(SimulationClock {
                current_instant: start_instant,
            })),
            end_instant,
            handlers: HashMap::new(),
        }
    }

    pub fn run(mut self) {
        self.events_queue.write().unwrap().push(EventAtInstant {
            target_actor_name: Arc::new("".to_string()),
            event: Box::new(SimulationEnd()),
            instant: self.end_instant,
        });

        while !self.events_queue.read().unwrap().is_empty() {
            let e = self.events_queue.write().unwrap().pop().unwrap();
            self.clock.write().unwrap().current_instant = e.instant;

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
    actor_name: Arc<String>,
    events_queue: Arc<RwLock<BinaryHeap<EventAtInstant>>>,
    clock: Arc<RwLock<SimulationClock>>,
    message_type: PhantomData<M>,
}

impl<M: 'static + Sync + Send> ActorRef<M> for SimulationActor<M> {
    fn async_send(&self, message: M) {
        self.events_queue.write().unwrap().push(EventAtInstant {
            target_actor_name: self.actor_name.clone(),
            event: Box::new(message),
            instant: self.clock.read().unwrap().current_instant,
        });
    }
}

impl ActorSystem for Simulation {
    fn spawn_actor<
        Msg: 'static + Sync + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &mut self,
        name: String,
        handler: MH,
    ) -> Box<dyn ActorRef<Msg> + Send> {
        let shared_actor_name = Arc::new(name);
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
