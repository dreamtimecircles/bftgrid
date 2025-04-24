//! An actor framework geared towards BFT ordering libraries with support for deterministic simulation testing.
//! It also allows writing a generic overall system construction logic independent from the specific actor system used.
//! Every actor is managed by a single actor system but can interact with actors managed by other actor systems.
//!
//! No async signatures are used in the public API in order to support single-threaded simulation testing without relying on async runtimes.
//! Actors should not assume anything about the thread they are running on, nor use any async runtime. They must rely on actor system facilities
//! to execute thread-blocking and async tasks.

use std::{
    any::Any,
    error::Error,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    io,
    sync::Arc,
    time::Duration,
};

use dyn_clone::{DynClone, clone_trait_object};
use thiserror::Error;
use tokio::task::JoinError;

pub trait ActorMsg: DynClone + Any + Send + Debug {}
clone_trait_object!(ActorMsg);

pub enum ActorControl {
    Exit(),
}

/// A [`TypedHandler`] is an actor that can handle messages of a specific type
/// and optionally return an [`ActorControl`] message.
pub trait TypedHandler: Send + Debug {
    type MsgT: ActorMsg;

    fn receive(&mut self, message: Self::MsgT) -> Option<ActorControl>;
}

#[derive(Debug, Clone)]
pub struct MessageNotSupported();

impl Display for MessageNotSupported {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "Message not supported")
    }
}

impl Error for MessageNotSupported {}

pub type AnActorMsg = Box<dyn ActorMsg>;

/// An [`UntypedHandler`] is an actor handler that can receive messages of any type,
/// although it may refuse to handle some of them.
pub trait UntypedHandler: Send + Debug {
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

pub type UntypedHandlerBox = Box<dyn UntypedHandler>;

/// A blanket [`UntypedHandler`] implementation for [`TypedHandler`]
/// to allow any typed actor to be used as a network input actor.
impl<MsgT, HandlerT> UntypedHandler for HandlerT
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT>,
{
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported> {
        match (message as Box<dyn Any>).downcast::<HandlerT::MsgT>() {
            Ok(typed_message) => Result::Ok(self.receive(*typed_message)),
            Err(_) => Result::Err(MessageNotSupported()),
        }
    }
}

/// A [`Task`] can be queried for completion.
pub trait Task: Send + Debug {
    fn is_finished(&self) -> bool;
}

/// A [`Joinable`] can be awaited for completion in a thread-blocking fashion.
/// Specific types of [`ActorRef`] and [`ActorSystem`] can be joined to wait for their completion.
pub trait Joinable<Output>: Task + Send + Debug {
    fn join(self) -> Output;
}

pub type AnActorRef<MsgT, HandlerT> = Box<dyn ActorRef<MsgT, HandlerT>>;

/// An [`ActorRef`] can asynchronously send messages to the underlying actor, optionally with a delay, and create new actor references.
/// Actor implementations can use [`ActorRef`]s to send messages to themselves and to other actors.
pub trait ActorRef<MsgT, HandlerT>: Send + Debug
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>);
    fn new_ref(&self) -> AnActorRef<MsgT, HandlerT>;
}

impl<MsgT, HandlerT> Clone for AnActorRef<MsgT, HandlerT>
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT> + 'static,
{
    fn clone(&self) -> Self {
        self.new_ref()
    }
}

/// An [`ActorSystem`] allows spawning actors by creating an [`ActoRef`] and setting its handler.
/// The handler of the underlying actor can also be changed at any time.
/// An [`ActorSystem`] can be cloned to obtain new references to it.
/// Actors can use [`ActorSystem`] references to spawn new actors and even to change their own handlers.
pub trait ActorSystem: Clone {
    type ActorRefT<MsgT, HandlerT>: ActorRef<MsgT, HandlerT>
    where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        node_id: impl Into<String>,
        name: impl Into<String>,
    ) -> Self::ActorRefT<MsgT, HandlerT>
    where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT>;

    fn set_handler<MsgT, HandlerT>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: ActorMsg,
        HandlerT: TypedHandler<MsgT = MsgT>;

    fn spawn_async_send<MsgT, HandlerT, O>(
        &self,
        f: impl Future<Output = O> + Send + 'static,
        to_msg: impl FnOnce(O) -> MsgT + Send + 'static,
        actor_ref: AnActorRef<MsgT, HandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static;

    fn spawn_blocking_send<MsgT, HandlerT, R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
        to_msg: impl FnOnce(R) -> MsgT + Send + 'static,
        actor_ref: AnActorRef<MsgT, HandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<MsgT = MsgT> + 'static,
        R: Send + 'static;
}

#[derive(Error, Debug, Clone)]
pub enum P2PNetworkError {
    #[error("I/O error")]
    Io(#[from] Arc<io::Error>),
    #[error("Join error")]
    Join(#[from] Arc<JoinError>),
    #[error("Actor not found")]
    ActorNotFound(Arc<String>),
}

pub type P2PNetworkResult<R> = Result<R, P2PNetworkError>;

/// A [`P2PNetwork`] allows sending messages to other nodes in a P2P network.
pub trait P2PNetworkClient: Clone {
    fn attempt_send<MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        serializer: &SerializerT,
        node: impl Into<String>,
    ) -> P2PNetworkResult<()>
    where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync;
}
