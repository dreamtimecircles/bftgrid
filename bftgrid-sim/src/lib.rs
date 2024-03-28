use std::{
    collections::{BinaryHeap, HashMap},
    error::Error,
    fmt::Debug,
    marker::PhantomData,
    mem,
    ops::Add,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bftgrid_core::{
    ActorControl, ActorMsg, ActorRef, ActorSystem, P2PNetwork, TypedHandler, UntypedHandlerBox,
};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

const SEED: u64 = 10;
const MAX_RANDOM_DURATION: Duration = Duration::from_secs(1);

pub struct SimulatedClock {
    current_instant: Instant,
}

#[derive(Clone, Debug)]
pub struct InternalEvent {
    node: Arc<String>,
    handler: Arc<String>,
    event: Box<dyn ActorMsg>,
    delay: Option<Duration>,
}

#[derive(Clone, Debug)]
pub enum SimulationEvent {
    ClientSend {
        // Injected before the simulation starts, is assigned an instant and reinjected as ClientRequest
        to_node: Arc<String>,
        event: Box<dyn ActorMsg>,
    },
    ClientRequest {
        // Sent as internal event to a node's client_request_handler
        to_node: Arc<String>,
        event: Box<dyn ActorMsg>,
    },
    P2PSend {
        // Produced by the simulated network, is assigned an instant and reinjected as P2PRequest
        to_node: Arc<String>,
        event: Box<dyn ActorMsg>,
    },
    P2PRequest {
        // Sent as internal event to a node's p2p_request_handler
        node: Arc<String>,
        event: Box<dyn ActorMsg>,
    },
    Internal {
        // Produced by internal modules for internal modules
        event: InternalEvent,
    },
    SimulationEnd,
}

#[derive(Clone, Debug)]
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

impl Ord for SimulationEventAtInstant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.instant.cmp(&self.instant) // Reverse order: earlier is bigger
    }
}

impl PartialOrd for SimulationEventAtInstant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct NodeDescriptor {
    pub client_request_handler: Option<Arc<String>>,
    pub p2p_request_handler: Option<Arc<String>>,
    pub all_handlers: HashMap<Arc<String>, Arc<Mutex<Option<UntypedHandlerBox>>>>,
}

impl NodeDescriptor {
    pub fn new(
        client_request_handler: Option<Arc<String>>,
        p2p_request_handler: Option<Arc<String>>,
    ) -> Self {
        NodeDescriptor {
            client_request_handler,
            p2p_request_handler,
            all_handlers: Default::default(),
        }
    }
}

impl Default for NodeDescriptor {
    fn default() -> Self {
        Self::new(Default::default(), Default::default())
    }
}

type Topology = HashMap<String, NodeDescriptor>;

pub struct SimulatedActor<MsgT, HandlerT> {
    topology: Arc<Mutex<Topology>>,
    node_id: Arc<String>,
    name: Arc<String>,
    handler: Arc<Mutex<Option<UntypedHandlerBox>>>,
    events_buffer: Arc<Mutex<Vec<InternalEvent>>>,
    message_type: PhantomData<MsgT>,
    handler_type: PhantomData<HandlerT>,
}

impl<MsgT, HandlerT> std::fmt::Debug for SimulatedActor<MsgT, HandlerT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulatedActor")
            .field("node_id", &self.node_id)
            .field("name", &self.name)
            .finish()
    }
}

#[async_trait]
impl<MsgT, HandlerT> ActorRef<MsgT, HandlerT> for SimulatedActor<MsgT, HandlerT>
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) {
        self.events_buffer.lock().unwrap().push(InternalEvent {
            node: self.node_id.clone(),
            handler: self.name.clone(),
            event: Box::new(message),
            delay,
        });
    }

    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>> {
        Box::new(SimulatedActor {
            topology: self.topology.clone(),
            node_id: self.node_id.clone(),
            name: self.name.clone(),
            handler: self.handler.clone(),
            events_buffer: self.events_buffer.clone(),
            message_type: self.message_type,
            handler_type: self.handler_type,
        })
    }
}

pub struct Simulation {
    topology: Arc<Mutex<Topology>>,
    exited_actors: Arc<Mutex<Vec<Arc<String>>>>,
    internal_events_buffer: Arc<Mutex<Vec<InternalEvent>>>,
    events_queue: Arc<Mutex<BinaryHeap<SimulationEventAtInstant>>>,
    clock: Arc<Mutex<SimulatedClock>>,
    random: ChaCha8Rng,
    end_instant: Instant,
}

impl Simulation {
    pub fn new(topology: Topology, start_instant: Instant, end_instant: Instant) -> Simulation {
        Simulation {
            topology: Arc::new(Mutex::new(topology)),
            exited_actors: Default::default(),
            internal_events_buffer: Default::default(),
            events_queue: Default::default(),
            clock: Arc::new(Mutex::new(SimulatedClock {
                current_instant: start_instant,
            })),
            random: ChaCha8Rng::seed_from_u64(SEED),
            end_instant,
        }
    }

    pub fn client_send<MsgT>(&mut self, to_node: String, message: MsgT)
    where
        MsgT: ActorMsg + 'static,
    {
        let instant = self.instant_of_client_request_send();
        self.events_queue
            .lock()
            .unwrap()
            .push(SimulationEventAtInstant {
                instant,
                event: SimulationEvent::ClientSend {
                    to_node: Arc::new(to_node),
                    event: Box::new(message),
                },
            })
    }

    pub fn run(mut self) -> Vec<SimulationEventAtInstant> {
        let mut result = Vec::new();
        self.events_queue
            .lock()
            .unwrap()
            .push(SimulationEventAtInstant {
                instant: self.end_instant,
                event: SimulationEvent::SimulationEnd,
            });

        while !self.events_queue.lock().unwrap().is_empty() {
            let mut mutex_buffer = self.internal_events_buffer.lock().unwrap();
            let mut events_buffer = Vec::new();
            mem::swap(&mut *mutex_buffer, &mut events_buffer);
            drop(mutex_buffer);
            for event in events_buffer {
                let instant = self.instant_of_internal_event();
                self.events_queue
                    .lock()
                    .unwrap()
                    .push(SimulationEventAtInstant {
                        instant,
                        event: SimulationEvent::Internal { event },
                    })
            }

            let e = self.events_queue.lock().unwrap().pop().unwrap();
            let e_clone = e.clone();
            result.push(e_clone);
            self.clock.lock().unwrap().current_instant = e.instant;

            match e.event {
                SimulationEvent::ClientSend { to_node, event } => {
                    let client_request_arrival_instant = self.instant_of_client_request_arrival();
                    self.events_queue
                        .lock()
                        .unwrap()
                        .push(SimulationEventAtInstant {
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
                        .unwrap_or_else(|| panic!("node {:?} unknown", to_node))
                        .client_request_handler
                        .as_ref()
                        .unwrap_or_else(|| {
                            panic!("client request handler unset for node {:?}", to_node)
                        })
                        .clone();
                    self.events_queue
                        .lock()
                        .unwrap()
                        .push(SimulationEventAtInstant {
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
                    self.events_queue
                        .lock()
                        .unwrap()
                        .push(SimulationEventAtInstant {
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
                        .unwrap_or_else(|| panic!("p2p request handler unset for node {:?}", node))
                        .clone();
                    self.events_queue
                        .lock()
                        .unwrap()
                        .push(SimulationEventAtInstant {
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
                    Some(duration) => {
                        self.events_queue
                            .lock()
                            .unwrap()
                            .push(SimulationEventAtInstant {
                                instant: e.instant.add(duration),
                                event: SimulationEvent::Internal {
                                    event: InternalEvent {
                                        node,
                                        handler,
                                        event,
                                        delay: None,
                                    },
                                },
                            });
                    }
                    None => {
                        if self
                            .exited_actors
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|elem| *elem == handler)
                        {
                            panic!("message sent to actor that has exited")
                        }
                        let mut topology = self.topology.lock().unwrap();
                        let mut removed_node = topology
                            .remove(node.as_ref())
                            .unwrap_or_else(|| panic!("node {:?} unknown", node));
                        let removed_handler_arc = removed_node
                            .all_handlers
                            .remove(&handler)
                            .unwrap_or_else(|| {
                                panic!("handler {:?} not found for node {:?}", handler, node)
                            });
                        topology.insert((*node).clone(), removed_node);
                        drop(topology); // So that a handler with access to the actor system can lock it again in `crate` and/or `set_handler`
                        if let Some(control) = removed_handler_arc
                            .lock()
                            .unwrap()
                            .as_mut()
                            .unwrap_or_else(|| {
                                panic!("handler {:?} not found for node {:?}", handler, node)
                            })
                            .receive_untyped(event.clone())
                            .unwrap_or_else(|_| {
                                panic!(
                                    "handler {:?} for node {:?} cannot handle event {:?}",
                                    handler, node, event
                                )
                            })
                        {
                            match control {
                                ActorControl::Exit() => {
                                    self.exited_actors.lock().unwrap().push(handler)
                                }
                            }
                        } else {
                            let mut topology = self.topology.lock().unwrap();
                            let mut removed_node = topology
                                .remove(node.as_ref())
                                .unwrap_or_else(|| panic!("node {:?} not found", node));
                            removed_node
                                .all_handlers
                                .insert(handler, removed_handler_arc.clone());
                            topology.insert((*node).clone(), removed_node);
                        };
                    }
                },
                SimulationEvent::SimulationEnd => break,
            }
        }
        result
    }

    fn instant_of_internal_event(&mut self) -> Instant {
        let pseudo_random_between_zero_and_one = self.random.next_u64() as f64 / u64::MAX as f64;
        let pseudo_random_duration =
            MAX_RANDOM_DURATION.mul_f64(pseudo_random_between_zero_and_one);
        self.clock
            .lock()
            .unwrap()
            .current_instant
            .add(pseudo_random_duration)
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

impl Clone for Simulation {
    fn clone(&self) -> Self {
        Simulation {
            topology: self.topology.clone(),
            exited_actors: self.exited_actors.clone(),
            internal_events_buffer: self.internal_events_buffer.clone(),
            events_queue: self.events_queue.clone(),
            clock: self.clock.clone(),
            random: self.random.clone(),
            end_instant: self.end_instant,
        }
    }
}

impl std::fmt::Debug for Simulation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Simulation")
            .field("topology", &self.topology)
            .field("random", &self.random)
            .field("end_instant", &self.end_instant)
            .finish()
    }
}

impl ActorSystem for Simulation {
    type ActorRefT<
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    > = SimulatedActor<MsgT, HandlerT>;

    fn create<MsgT, HandlerT>(
        &mut self,
        node_id: String,
        name: String,
    ) -> SimulatedActor<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    {
        let handler_rc = Arc::new(Mutex::new(None));
        let name_arc = Arc::new(name);
        let name_arc2 = name_arc.clone();
        if self
            .topology
            .lock()
            .unwrap()
            .get_mut(&node_id)
            .unwrap_or_else(|| panic!("Node {:?} unknown", &node_id))
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
            message_type: PhantomData {},
            handler_type: PhantomData {},
        }
    }

    fn set_handler<MsgT, HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
    {
        let mut topology = self.topology.lock().unwrap();
        let node = topology.get_mut(actor_ref.node_id.as_ref()).unwrap();
        node.all_handlers.insert(
            actor_ref.name.clone(),
            Arc::new(Mutex::new(Some(Box::new(handler)))),
        );
    }
}

#[async_trait]
impl P2PNetwork for Simulation {
    fn send<const BUFFER_SIZE: usize, MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        _serializer: &SerializerT,
        to_node: &str,
    ) where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT, &mut [u8]) -> Result<usize, Box<dyn Error>> + Sync,
    {
        let instant = self.instant_of_p2p_request_send();
        self.events_queue
            .lock()
            .unwrap()
            .push(SimulationEventAtInstant {
                instant,
                event: SimulationEvent::P2PSend {
                    to_node: Arc::new(to_node.into()),
                    event: Box::new(message),
                },
            })
    }

    fn broadcast<const BUFFER_SIZE: usize, MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        _serializer: &SerializerT,
    ) where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT, &mut [u8]) -> Result<usize, Box<dyn Error>> + Sync,
    {
        let instant = self.instant_of_p2p_request_send();
        for node_name in self.topology.lock().unwrap().keys() {
            self.events_queue
                .lock()
                .unwrap()
                .push(SimulationEventAtInstant {
                    instant,
                    event: SimulationEvent::P2PSend {
                        to_node: Arc::new(node_name.clone()),
                        event: dyn_clone::clone_box(&message),
                    },
                })
        }
    }
}
