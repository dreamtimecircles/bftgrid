use std::{
    any::Any,
    cell::RefCell,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    ops::Add,
    ptr,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bft_grid_core::{
    Joinable, P2PNetwork, SingleThreadedActorRef, SingleThreadedActorSystem, TypedMessageHandler,
    UntypedMessageHandler,
};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

const SEED: u64 = 10;
const MAX_RANDOM_DURATION: Duration = Duration::from_secs(1);

pub struct SimulatedClock {
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
    ) -> Self {
        Node {
            client_request_handler: Rc::new(RefCell::new(client_request_handler)),
            p2p_request_handler: Rc::new(RefCell::new(p2p_request_handler)),
            internal_handlers: HashMap::new(),
        }
    }
}

type Topology = HashMap<String, Node>;

pub struct Simulation {
    pub topology: Topology,

    exited: Vec<Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>>,
    internal_events_buffer: Rc<RefCell<Vec<InternalEvent>>>,
    events_queue: BinaryHeap<SimulationEventAtInstant>,
    clock: SimulatedClock,
    end_instant: Instant,
    random: ChaCha8Rng,
}

impl Simulation {
    pub fn new(topology: Topology, start_instant: Instant, end_instant: Instant) -> Simulation {
        Simulation {
            topology,
            exited: Vec::new(),
            internal_events_buffer: Rc::new(RefCell::new(Vec::new())),
            events_queue: BinaryHeap::new(),
            clock: SimulatedClock {
                current_instant: start_instant,
            },
            end_instant,
            random: ChaCha8Rng::seed_from_u64(SEED),
        }
    }

    pub fn client_send<Msg: 'static>(&mut self, to_node: String, message: Msg) {
        let instant = self.instant_of_client_request_send();
        self.events_queue.push(SimulationEventAtInstant {
            instant,
            event: SimulationEvent::ClientSend {
                to_node: Rc::new(to_node),
                event: Box::new(message),
            },
        })
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
                    None => {
                        if self
                            .exited
                            .iter()
                            .any(|elem| ptr::eq((**elem).as_ptr(), to_handler.as_ptr()))
                        {
                            panic!("Message sent to actor that has exited")
                        }

                        if let Some(control) = to_handler
                            .borrow_mut()
                            .receive_untyped(event)
                            .expect("Found event targeting the wrong actor")
                        {
                            match control {
                                bft_grid_core::ActorControl::Exit() => {
                                    self.exited.push(to_handler.clone())
                                }
                            }
                        }
                    }
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

    fn instant_of_client_request_send(&mut self) -> Instant {
        self.instant_of_internal_event()
    }

    fn instant_of_client_request_arrival(&mut self) -> Instant {
        self.instant_of_internal_event()
    }

    fn instant_of_p2p_request_send(&mut self) -> Instant {
        self.instant_of_internal_event()
    }

    fn instant_of_p2p_request_arrival(&mut self) -> Instant {
        self.instant_of_internal_event()
    }
}

pub struct SimulatedActor<Msg> {
    handler: Rc<RefCell<Box<dyn UntypedMessageHandler<'static>>>>,
    events_buffer: Rc<RefCell<Vec<InternalEvent>>>,
    message_type: PhantomData<Msg>,
}

struct Ready {}

impl Joinable<Option<()>> for Ready {
    fn join(self) -> Option<()> {
        Some(())
    }

    fn is_finished(&mut self) -> bool {
        true
    }
}

#[async_trait]
impl<Msg> SingleThreadedActorRef<Msg> for SimulatedActor<Msg>
where
    Msg: 'static,
{
    fn send(&mut self, message: Msg, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>> {
        self.events_buffer.borrow_mut().push(InternalEvent {
            to_handler: self.handler.clone(),
            event: Box::new(message),
            delay,
        });
        Box::new(Ready {})
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
        Box::new(SimulatedActor {
            handler: handler_rc,
            events_buffer: self.internal_events_buffer.clone(),
            message_type: PhantomData {},
        })
    }
}

impl P2PNetwork for Simulation {
    fn send<Msg: 'static>(&mut self, message: Msg, to_node: String) {
        let instant = self.instant_of_p2p_request_send();
        self.events_queue.push(SimulationEventAtInstant {
            instant,
            event: SimulationEvent::ClientSend {
                to_node: Rc::new(to_node),
                event: Box::new(message),
            },
        })
    }

    fn broadcast<Msg: Clone + 'static>(&mut self, message: Msg) {
        let instant = self.instant_of_p2p_request_send();
        for node_name in self.topology.keys() {
            self.events_queue.push(SimulationEventAtInstant {
                instant,
                event: SimulationEvent::ClientSend {
                    to_node: Rc::new(node_name.clone()),
                    event: Box::new(message.clone()),
                },
            })
        }
    }
}
