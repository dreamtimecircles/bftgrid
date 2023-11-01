use std::{
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
    mem,
    ops::Add,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bftgrid_core::{
    ActorControl, ActorRef, ActorSystem, BoxClone, P2PNetwork, TypedHandler, UntypedHandler,
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

#[derive(Debug)]
pub struct InternalEvent {
    node: Arc<String>,
    handler: Arc<String>,
    event: Box<dyn BoxClone + Send>,
    delay: Option<Duration>,
}

impl Clone for InternalEvent {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            handler: self.handler.clone(),
            event: self.event.box_clone(),
            delay: self.delay,
        }
    }
}

#[derive(Debug)]
pub enum SimulationEvent {
    ClientSend {
        // Injected before the simulation starts, is assigned an instant and reinjected as ClientRequest
        to_node: Arc<String>,
        event: Box<dyn BoxClone + Send>,
    },
    ClientRequest {
        // Sent as internal event to a node's client_request_handler
        to_node: Arc<String>,
        event: Box<dyn BoxClone + Send>,
    },
    P2PSend {
        // Produced by the simulated network, is assigned an instant and reinjected as P2PRequest
        to_node: Arc<String>,
        event: Box<dyn BoxClone + Send>,
    },
    P2PRequest {
        // Sent as internal event to a node's p2p_request_handler
        node: Arc<String>,
        event: Box<dyn BoxClone + Send>,
    },
    Internal {
        // Produced by internal modules for internal modules
        event: InternalEvent,
    },
    SimulationEnd,
}

impl Clone for SimulationEvent {
    fn clone(&self) -> Self {
        match self {
            Self::ClientSend { to_node, event } => Self::ClientSend {
                to_node: to_node.clone(),
                event: event.box_clone(),
            },
            Self::ClientRequest { to_node, event } => Self::ClientRequest {
                to_node: to_node.clone(),
                event: event.box_clone(),
            },
            Self::P2PSend { to_node, event } => Self::P2PSend {
                to_node: to_node.clone(),
                event: event.box_clone(),
            },
            Self::P2PRequest { node, event } => Self::P2PRequest {
                node: node.clone(),
                event: event.box_clone(),
            },
            Self::Internal { event } => Self::Internal {
                event: event.clone(),
            },
            Self::SimulationEnd => Self::SimulationEnd,
        }
    }
}

#[derive(Debug)]
pub struct SimulationEventAtInstant {
    instant: Instant,
    event: SimulationEvent,
}

impl Clone for SimulationEventAtInstant {
    fn clone(&self) -> Self {
        Self {
            instant: self.instant,
            event: self.event.clone(),
        }
    }
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

#[derive(Debug)]
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
    MsgT: BoxClone + Send + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static,
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

type Topology = HashMap<String, Node>;

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
            exited_actors: Arc::new(Mutex::new(Vec::new())),
            internal_events_buffer: Arc::new(Mutex::new(Vec::new())),
            events_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            clock: Arc::new(Mutex::new(SimulatedClock {
                current_instant: start_instant,
            })),
            random: ChaCha8Rng::seed_from_u64(SEED),
            end_instant,
        }
    }

    pub fn client_send<MsgT>(&mut self, to_node: String, message: MsgT)
    where
        MsgT: BoxClone + Send + 'static,
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
                        .unwrap()
                        .client_request_handler
                        .as_ref()
                        .expect("client request handler unset")
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
                        .expect("P2P request handler unset")
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
                            panic!("Message sent to actor that has exited")
                        }

                        let mut topology = self.topology.lock().unwrap();
                        let mut removed_node =
                            topology.remove(node.as_ref()).expect("node not found");
                        let removed_handler_arc = removed_node
                            .all_handlers
                            .remove(&handler)
                            .expect("handler not found");
                        topology.insert((*node).clone(), removed_node);
                        drop(topology); // So that a handler with access to the actor system can lock it again in `crate` and/or `set_handler`
                        if let Some(control) = removed_handler_arc
                            .lock()
                            .unwrap()
                            .as_mut()
                            .expect("handler not set")
                            .receive_untyped(event.any_clone())
                            .expect("Found event targeting the wrong actor")
                        {
                            match control {
                                ActorControl::Exit() => {
                                    self.exited_actors.lock().unwrap().push(handler)
                                }
                            }
                        } else {
                            let mut topology = self.topology.lock().unwrap();
                            let mut removed_node =
                                topology.remove(node.as_ref()).expect("node not found");
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
        MsgT: BoxClone + Send + 'static,
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
            message_type: PhantomData {},
            handler_type: PhantomData {},
        }
    }

    fn set_handler<MsgT, HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: BoxClone + Send + 'static,
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
        MsgT: BoxClone + Send + 'static,
    {
        let instant = self.instant_of_p2p_request_send();
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

    fn broadcast<MsgT>(&mut self, message: MsgT)
    where
        MsgT: BoxClone + Send + 'static,
    {
        let instant = self.instant_of_p2p_request_send();
        for node_name in self.topology.lock().unwrap().keys() {
            self.events_queue
                .lock()
                .unwrap()
                .push(SimulationEventAtInstant {
                    instant,
                    event: SimulationEvent::ClientSend {
                        to_node: Arc::new(node_name.clone()),
                        event: message.box_clone(),
                    },
                })
        }
    }
}
