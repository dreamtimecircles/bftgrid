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

/// A [`TypedMsgHandler`] is an actor behavior that can handle messages of a specific type
/// and optionally return an [`ActorControl`] message.
pub trait TypedMsgHandler: Send + Debug {
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

/// An [`UntypedMsgHandler`] is an actor handler that can receive messages of any type,
/// although it may refuse to handle some of them.
pub trait UntypedMsgHandler: Send + Debug {
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

pub type UntypedHandlerBox = Box<dyn UntypedMsgHandler>;

/// A blanket [`UntypedMsgHandler`] implementation for [`TypedMsgHandler`]
/// to allow any typed actor to be used as a network input actor.
impl<MsgT, MsgHandlerT> UntypedMsgHandler for MsgHandlerT
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT>,
{
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported> {
        match (message as Box<dyn Any>).downcast::<MsgHandlerT::MsgT>() {
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
/// Specific types of [`ActorRef`] and [`ActorSystemHandle`] can be joined to wait for their completion.
pub trait Joinable<Output>: Task + Send + Debug {
    fn join(self) -> Output;
}

pub type AnActorRef<MsgT, MsgHandlerT> = Box<dyn ActorRef<MsgT, MsgHandlerT>>;

/// An [`ActorRef`] can asynchronously send messages to the underlying actor, optionally with a delay, and create new actor references.
/// Actor implementations can use [`ActorRef`]s to send messages to themselves and to other actors.
pub trait ActorRef<MsgT, MsgHandlerT>: Send + Debug
where
    MsgT: ActorMsg + 'static,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>);
    fn new_ref(&self) -> AnActorRef<MsgT, MsgHandlerT>;
}

impl<MsgT, MsgHandlerT> Clone for AnActorRef<MsgT, MsgHandlerT>
where
    MsgT: ActorMsg,
    MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static,
{
    fn clone(&self) -> Self {
        self.new_ref()
    }
}

/// An [`ActorSystemHandle`] allows spawning actors by creating an [`ActorRef`] and setting its handler.
/// The handler of the underlying actor can also be changed at any time.
/// An [`ActorSystemHandle`] can be cloned.
/// Actors themselves can use [`ActorSystemHandle`]s to spawn new actors and even to change their own handlers.
pub trait ActorSystemHandle: Clone {
    type ActorRefT<MsgT, MsgHandlerT>: ActorRef<MsgT, MsgHandlerT>
    where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static;

    fn create<MsgT, MsgHandlerT>(
        &mut self,
        node_id: impl Into<String>,
        name: impl Into<String>,
    ) -> Self::ActorRefT<MsgT, MsgHandlerT>
    where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT>;

    fn set_handler<MsgT, MsgHandlerT>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, MsgHandlerT>,
        handler: MsgHandlerT,
    ) where
        MsgT: ActorMsg,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT>;

    fn spawn_async_send<MsgT, MsgHandlerT>(
        &mut self,
        f: impl Future<Output = MsgT> + Send + 'static,
        actor_ref: AnActorRef<MsgT, MsgHandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static;

    fn spawn_thread_blocking_send<MsgT, MsgHandlerT>(
        &mut self,
        f: impl FnOnce() -> MsgT + Send + 'static,
        actor_ref: AnActorRef<MsgT, MsgHandlerT>,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static,
        MsgHandlerT: TypedMsgHandler<MsgT = MsgT> + 'static;
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

/// A [`P2PNetworkClient`] allows sending messages to other nodes in a P2P network.
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
