use std::{
    any::Any,
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    mem,
    ops::Add,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bft_grid_core::{ActorRef, ActorSystem, Joinable, P2PNetwork, TypedHandler, UntypedHandler};
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
    node: Arc<String>,
    handler: Arc<String>,
    event: Box<dyn Any + Send>,
    delay: Option<Duration>,
}

pub enum SimulationEvent {
    ClientSend {
        // Injected before the simulation starts, is assigned an instant and reinjected as ClientRequest
        to_node: Arc<String>,
        event: Box<dyn Any + Send>,
    },
    ClientRequest {
        // Sent as internal event to a node's client_request_handler
        to_node: Arc<String>,
        event: Box<dyn Any + Send>,
    },
    P2PSend {
        // Produced by the simulated network, is assigned an instant and reinjected as P2PRequest
        to_node: Arc<String>,
        event: Box<dyn Any + Send>,
    },
    P2PRequest {
        // Sent as internal event to a node's p2p_request_handler
        node: Arc<String>,
        event: Box<dyn Any + Send>,
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

type UntypedHandlerBox = Box<dyn UntypedHandler<'static> + Send>;

pub struct Node {
    client_request_handler: Option<Arc<String>>,
    p2p_request_handler: Option<Arc<String>>,
    all_handlers: HashMap<Arc<String>, Arc<Mutex<Option<UntypedHandlerBox>>>>,
}

impl Node {
    pub fn new() -> Self {
        Node {
            client_request_handler: None,
            p2p_request_handler: None,
            all_handlers: HashMap::new(),
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

type Topology = HashMap<String, Node>;

pub struct Simulation {
    topology: Arc<Mutex<Topology>>,
    exited_actors: Vec<Arc<String>>,
    internal_events_buffer: Arc<Mutex<Vec<InternalEvent>>>,
    events_queue: BinaryHeap<SimulationEventAtInstant>,
    clock: SimulatedClock,
    random: ChaCha8Rng,
    end_instant: Instant,
    finished_cond: Arc<(Mutex<bool>, Condvar)>,
}

impl Simulation {
    pub fn new(topology: Topology, start_instant: Instant, end_instant: Instant) -> Simulation {
        Simulation {
            topology: Arc::new(Mutex::new(topology)),
            exited_actors: Vec::new(),
            internal_events_buffer: Arc::new(Mutex::new(Vec::new())),
            events_queue: BinaryHeap::new(),
            clock: SimulatedClock {
                current_instant: start_instant,
            },
            random: ChaCha8Rng::seed_from_u64(SEED),
            end_instant,
            finished_cond: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    pub fn client_send<Msg>(&mut self, to_node: String, message: Msg)
    where
        Msg: Send + 'static,
    {
        let instant = self.instant_of_client_request_send();
        self.events_queue.push(SimulationEventAtInstant {
            instant,
            event: SimulationEvent::ClientSend {
                to_node: Arc::new(to_node),
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
            let mut mutex_buffer = self.internal_events_buffer.lock().unwrap();
            let mut events_buffer = Vec::new();
            mem::swap(&mut *mutex_buffer, &mut events_buffer);
            drop(mutex_buffer);
            for event in events_buffer {
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
                        .lock()
                        .unwrap()
                        .get(to_node.as_ref())
                        .unwrap()
                        .client_request_handler
                        .as_ref()
                        .expect("client request handler unset")
                        .clone();
                    self.events_queue.push(SimulationEventAtInstant {
                        instant: internal_event_instant,
                        event: SimulationEvent::Internal {
                            event: InternalEvent {
                                node: to_node,
                                handler: client_request_handler,
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
                        .lock()
                        .unwrap()
                        .get(node.as_ref())
                        .unwrap()
                        .p2p_request_handler
                        .as_ref()
                        .expect("P2P request handler unset")
                        .clone();
                    self.events_queue.push(SimulationEventAtInstant {
                        instant: internal_event_instant,
                        event: SimulationEvent::Internal {
                            event: InternalEvent {
                                node,
                                handler: p2p_request_handler,
                                event,
                                delay: None,
                            },
                        },
                    })
                }
                SimulationEvent::Internal {
                    event:
                        InternalEvent {
                            node,
                            handler,
                            event,
                            delay,
                        },
                } => match delay {
                    Some(duration) => self.events_queue.push(SimulationEventAtInstant {
                        instant: e.instant.add(duration),
                        event: SimulationEvent::Internal {
                            event: InternalEvent {
                                node,
                                handler,
                                event,
                                delay: None,
                            },
                        },
                    }),
                    None => {
                        if self.exited_actors.iter().any(|elem| *elem == handler) {
                            panic!("Message sent to actor that has exited")
                        }

                        let topology = self.topology.lock().unwrap();
                        let handler_arc = topology
                            .get(node.as_ref())
                            .expect("node not found")
                            .all_handlers
                            .get(&handler)
                            .expect("handler not found");
                        if let Some(control) = handler_arc
                            .lock()
                            .unwrap()
                            .as_mut()
                            .expect("handler not set")
                            .receive_untyped(event)
                            .expect("Found event targeting the wrong actor")
                        {
                            match control {
                                bft_grid_core::ActorControl::Exit() => {
                                    self.exited_actors.push(handler)
                                }
                            }
                        };
                    }
                },
                SimulationEvent::SimulationEnd => {
                    let (finished_mutex, cvar) = &*self.finished_cond;
                    let mut finished = finished_mutex.lock().unwrap();
                    *finished = true;
                    cvar.notify_all();
                    break;
                }
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

pub struct SimulatedActor<MsgT, HandlerT> {
    topology: Arc<Mutex<Topology>>,
    node_id: Arc<String>,
    name: Arc<String>,
    handler: Arc<Mutex<Option<UntypedHandlerBox>>>,
    events_buffer: Arc<Mutex<Vec<InternalEvent>>>,
    simulation_finished_cond: Arc<(Mutex<bool>, Condvar)>,
    message_type: PhantomData<MsgT>,
    handler_type: PhantomData<HandlerT>,
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

impl<MsgT, HandlerT> Joinable<()> for SimulatedActor<MsgT, HandlerT> {
    fn join(self) {
        let (finished_mutex, cvar) = &*self.simulation_finished_cond;
        let mut finished = finished_mutex.lock().unwrap();
        while !*finished {
            finished = cvar.wait(finished).unwrap();
        }
    }

    fn is_finished(&mut self) -> bool {
        let (finished_mutex, _) = &*self.simulation_finished_cond;
        *finished_mutex.lock().unwrap()
    }
}

#[async_trait]
impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for SimulatedActor<MsgT, HandlerT>
where
    MsgT: Send + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>> {
        self.events_buffer.lock().unwrap().push(InternalEvent {
            node: self.node_id.clone(),
            handler: self.name.clone(),
            event: Box::new(message),
            delay,
        });
        Box::new(Ready {})
    }

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>> {
        Box::new(SimulatedActor {
            topology: self.topology.clone(),
            node_id: self.node_id.clone(),
            name: self.name.clone(),
            handler: self.handler.clone(),
            events_buffer: self.events_buffer.clone(),
            simulation_finished_cond: self.simulation_finished_cond.clone(),
            message_type: self.message_type,
            handler_type: self.handler_type,
        })
    }
}

impl ActorSystem for Simulation {
    type ConcreteActorRef<
        MsgT: Send + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
    > = SimulatedActor<MsgT, HandlerT>;

    fn create<MsgT, HandlerT>(
        &mut self,
        node_id: String,
        name: String,
    ) -> SimulatedActor<MsgT, HandlerT>
    where
        MsgT: Send + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
    {
        let handler_rc = Arc::new(Mutex::new(None));
        let name_arc = Arc::new(name);
        let name_arc2 = name_arc.clone();
        if self
            .topology
            .lock()
            .unwrap()
            .get_mut(&node_id)
            .unwrap()
            .all_handlers
            .insert(name_arc, handler_rc.clone())
            .is_some()
        {
            panic!("An actor with such a name already exist");
        }
        SimulatedActor {
            topology: self.topology.clone(),
            node_id: Arc::new(node_id),
            name: name_arc2,
            handler: handler_rc,
            events_buffer: self.internal_events_buffer.clone(),
            simulation_finished_cond: self.finished_cond.clone(),
            message_type: PhantomData {},
            handler_type: PhantomData {},
        }
    }

    fn set_handler<MsgT, HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static>(
        &mut self,
        actor_ref: &mut Self::ConcreteActorRef<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: 'static + Send,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
    {
        let mut topology = self.topology.lock().unwrap();
        let node = topology.get_mut(actor_ref.node_id.as_ref()).unwrap();
        node.all_handlers.insert(
            actor_ref.name.clone(),
            Arc::new(Mutex::new(Some(Box::new(handler)))),
        );
    }
}

impl P2PNetwork for Simulation {
    fn send<MsgT>(&mut self, message: MsgT, to_node: String)
    where
        MsgT: Send + 'static,
    {
        let instant = self.instant_of_p2p_request_send();
        self.events_queue.push(SimulationEventAtInstant {
            instant,
            event: SimulationEvent::ClientSend {
                to_node: Arc::new(to_node),
                event: Box::new(message),
            },
        })
    }

    fn broadcast<MsgT>(&mut self, message: MsgT)
    where
        MsgT: Clone + Send + 'static,
    {
        let instant = self.instant_of_p2p_request_send();
        for node_name in self.topology.lock().unwrap().keys() {
            self.events_queue.push(SimulationEventAtInstant {
                instant,
                event: SimulationEvent::ClientSend {
                    to_node: Arc::new(node_name.clone()),
                    event: Box::new(message.clone()),
                },
            })
        }
    }
}
