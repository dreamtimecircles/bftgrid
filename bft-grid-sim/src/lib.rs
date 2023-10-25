use std::{
    any::Any,
    cell::RefCell,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    ops::Add,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bft_grid_core::{
    SingleThreadedActorRef, SingleThreadedActorSystem, TypedMessageHandler, UntypedMessageHandler,
};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

pub struct SimulationClock {
    pub current_instant: Instant,
}

pub struct InternalEvent {
    to_handler: Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>,
    event: Box<dyn Any>,
    delay: Option<Duration>,
}

pub enum SimulationEvent {
    ClientSend {
        // Injected before the simulation starts, is assigned an instant and reinjected as ClientRequest
        to_node: Rc<String>,
        event: Box<dyn Any>,
    },
    ClientRequest {
        // Sent as internal event to a node's client_request_handler
        to_node: Rc<String>,
        event: Box<dyn Any>,
    },
    P2PSend {
        // Produced by the simulated network, is assigned an instant and reinjected as P2PRequest
        to_node: Rc<String>,
        event: Box<dyn Any>,
    },
    P2PRequest {
        // Sent as internal event to a node's p2p_request_handler
        node: Rc<String>,
        event: Box<dyn Any>,
    },
    Internal {
        // Produced by internal modules for internal modules
        event: InternalEvent,
    },
    SimulationEnd,
}

pub struct SimulationEventAtInstant {
    instant: Instant,
    event: SimulationEvent,
}

impl PartialEq for SimulationEventAtInstant {
    fn eq(&self, other: &Self) -> bool {
        self.instant == other.instant
    }
}

impl Eq for SimulationEventAtInstant {}

impl PartialOrd for SimulationEventAtInstant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.instant.partial_cmp(&self.instant) // Reverse order: earlier is bigger
    }
}

impl Ord for SimulationEventAtInstant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.instant.cmp(&self.instant) // Reverse order: earlier is bigger
    }
}

pub struct Node {
    client_request_handler: Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>,
    p2p_request_handler: Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>,
    internal_handlers: HashMap<Rc<String>, Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>>,
}

impl Node {
    pub fn new(
        client_request_handler: Box<dyn UntypedMessageHandler<'static>>,
        p2p_request_handler: Box<dyn UntypedMessageHandler<'static>>,
        internal_handlers: HashMap<String, Box<dyn UntypedMessageHandler<'static>>>,
    ) -> Self {
        let mut handlers =
            HashMap::<Rc<String>, Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>>::new();
        for (handler_name, handler) in internal_handlers {
            handlers.insert(Rc::new(handler_name), Rc::new(RefCell::new(handler)));
        }
        Node {
            client_request_handler: Rc::new(RefCell::new(client_request_handler)),
            p2p_request_handler: Rc::new(RefCell::new(p2p_request_handler)),
            internal_handlers: handlers,
        }
    }
}

type Topology = HashMap<String, Node>;

const SEED: u64 = 10;
const MAX_RANDOM_DURATION: Duration = Duration::from_secs(1);

pub struct Simulation {
    pub topology: Topology,

    internal_events_buffer: Rc<RefCell<Vec<InternalEvent>>>,
    events_queue: BinaryHeap<SimulationEventAtInstant>,
    clock: SimulationClock,
    end_instant: Instant,
    random: ChaCha8Rng,
}

impl Simulation {
    pub fn new(topology: Topology, start_instant: Instant, end_instant: Instant) -> Simulation {
        Simulation {
            topology,
            internal_events_buffer: Rc::new(RefCell::new(Vec::new())),
            events_queue: BinaryHeap::new(),
            clock: SimulationClock {
                current_instant: start_instant,
            },
            end_instant,
            random: ChaCha8Rng::seed_from_u64(SEED),
        }
    }

    pub fn run(mut self) {
        let instant = self.end_instant;
        self.events_queue.push(SimulationEventAtInstant {
            instant,
            event: SimulationEvent::SimulationEnd,
        });

        while !self.events_queue.is_empty() {
            for event in self.internal_events_buffer.replace(Vec::new()) {
                let instant = self.instant_of_internal_event();
                self.events_queue.push(SimulationEventAtInstant {
                    instant,
                    event: SimulationEvent::Internal { event },
                })
            }

            let e = self.events_queue.pop().unwrap();
            self.clock.current_instant = e.instant;

            match e.event {
                SimulationEvent::ClientSend { to_node, event } => {
                    let client_request_arrival_instant = self.instant_of_client_request_arrival();
                    self.events_queue.push(SimulationEventAtInstant {
                        instant: client_request_arrival_instant,
                        event: SimulationEvent::ClientRequest { to_node, event },
                    })
                }
                SimulationEvent::ClientRequest { to_node, event } => {
                    let internal_event_instant = self.instant_of_internal_event();
                    let client_request_handler = self
                        .topology
                        .get_mut(to_node.as_ref())
                        .unwrap()
                        .client_request_handler
                        .clone();
                    let to_handler = client_request_handler.clone();
                    self.events_queue.push(SimulationEventAtInstant {
                        instant: internal_event_instant,
                        event: SimulationEvent::Internal {
                            event: InternalEvent {
                                to_handler,
                                event,
                                delay: None,
                            },
                        },
                    })
                }
                SimulationEvent::P2PSend { to_node, event } => {
                    let p2p_request_arrival_instant = self.instant_of_p2p_request_arrival();
                    self.events_queue.push(SimulationEventAtInstant {
                        instant: p2p_request_arrival_instant,
                        event: SimulationEvent::P2PRequest {
                            node: to_node,
                            event,
                        },
                    });
                }
                SimulationEvent::P2PRequest { node, event } => {
                    let internal_event_instant = self.instant_of_internal_event();
                    let p2p_request_handler = self
                        .topology
                        .get_mut(node.as_ref())
                        .unwrap()
                        .p2p_request_handler
                        .clone();
                    let to_handler = p2p_request_handler.clone();
                    self.events_queue.push(SimulationEventAtInstant {
                        instant: internal_event_instant,
                        event: SimulationEvent::Internal {
                            event: InternalEvent {
                                to_handler,
                                event,
                                delay: None,
                            },
                        },
                    })
                }
                SimulationEvent::Internal {
                    event:
                        InternalEvent {
                            to_handler,
                            event,
                            delay,
                        },
                } => match delay {
                    Some(duration) => self.events_queue.push(SimulationEventAtInstant {
                        instant: e.instant.add(duration),
                        event: SimulationEvent::Internal {
                            event: InternalEvent {
                                to_handler,
                                event,
                                delay: None,
                            },
                        },
                    }),
                    None => to_handler
                        .borrow_mut()
                        .receive_untyped(event)
                        .expect("Found event targeting the wrong actor"),
                },
                SimulationEvent::SimulationEnd => return,
            }
        }
    }

    fn instant_of_internal_event(&mut self) -> Instant {
        let pseudo_random_between_zero_and_one = self.random.next_u64() as f64 / u64::MAX as f64;
        let pseudo_random_duration =
            MAX_RANDOM_DURATION.mul_f64(pseudo_random_between_zero_and_one);
        self.clock.current_instant.add(pseudo_random_duration)
    }

    fn instant_of_client_request_arrival(&mut self) -> Instant {
        todo!()
    }

    fn instant_of_p2p_request_arrival(&mut self) -> Instant {
        todo!()
    }
}

pub struct SimulationActor<M> {
    handler: Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>,
    events_buffer: Rc<RefCell<Vec<InternalEvent>>>,
    message_type: PhantomData<M>,
}

#[async_trait]
impl<Msg> SingleThreadedActorRef<Msg> for SimulationActor<Msg>
where
    Msg: 'static,
{
    fn send(&mut self, message: Msg, delay: Option<Duration>) {
        self.events_buffer.borrow_mut().push(InternalEvent {
            to_handler: self.handler.clone(),
            event: Box::new(message),
            delay,
        });
    }
}

impl SingleThreadedActorSystem for Simulation {
    fn spawn_actor<Msg, MessageHandler>(
        &mut self,
        node: String,
        actor_name: String,
        handler: MessageHandler,
    ) -> Box<dyn SingleThreadedActorRef<Msg>>
    where
        Msg: 'static,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg>,
    {
        let shared_node = Rc::new(node);
        let handler_rc = Rc::new(RefCell::new(
            Box::new(handler) as Box<dyn UntypedMessageHandler>
        ));
        if self
            .topology
            .get_mut(shared_node.as_ref())
            .unwrap()
            .internal_handlers
            .insert(Rc::new(actor_name), handler_rc.clone())
            .is_some()
        {
            panic!("An actor with such a name already exist");
        }
        Box::new(SimulationActor {
            handler: handler_rc,
            events_buffer: self.internal_events_buffer.clone(),
            message_type: PhantomData {},
        })
    }
}
