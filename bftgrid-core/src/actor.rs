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
pub trait TypedMsgHandler<MsgT>: Send + Debug
where
    MsgT: ActorMsg,
{
    fn receive(&mut self, message: MsgT) -> Option<ActorControl>;
}

pub type MsgHandler<MsgT> = Box<dyn TypedMsgHandler<MsgT>>;

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
///  although it may refuse to handle some of them.
pub trait UntypedMsgHandler: Send + Debug {
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

pub type UntypedHandlerBox = Box<dyn UntypedMsgHandler>;

/// A blanket [`UntypedMsgHandler`] implementation for any [`MsgHandler<MsgT>`]
///  to allow any boxed typed actor to be used as a network input actor.
// The manual supertrait upcasting approach
//  (https://quinedot.github.io/rust-learning/dyn-trait-combining.html#manual-supertrait-upcasting)
//  would not work in this case due to the `MsgT` type parameter being
//  method-bound rather than trait-bound in a blanket implementation for `T: TypedMsgHandler<MsgT>`.
//  Somewhat sadly, this means that a generic [`UntypedMsgHandler`] must be costructed via a second level
//  of boxing (`Box<dyn UntypedMsgHandler>`).
impl<MsgT> UntypedMsgHandler for MsgHandler<MsgT>
where
    MsgT: ActorMsg,
{
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported> {
        match (message as Box<dyn Any>).downcast::<MsgT>() {
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
    fn join(&mut self) -> Output;
}

/// An [`ActorRef`] can asynchronously send messages to the underlying actor, optionally with a delay, and create new actor references.
/// Actor implementations can use [`ActorRef`]s to send messages to themselves and to other actors.
pub trait ActorRef<MsgT>: Clone + Send + Debug
where
    MsgT: ActorMsg + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>);
}

/// An [`ActorSystemHandle`] allows spawning actors by creating an [`ActorRef`] and setting its handler.
/// The handler of the underlying actor can also be changed at any time.
/// An [`ActorSystemHandle`] can be cloned.
/// Actors themselves can use [`ActorSystemHandle`]s to spawn new actors and even to change their own handlers.
pub trait ActorSystemHandle: Clone {
    type ActorRefT<MsgT>: ActorRef<MsgT>
    where
        MsgT: ActorMsg;

    fn create<MsgT>(
        &self,
        node_id: impl Into<String>,
        name: impl Into<String>,
        join_on_drop: bool,
    ) -> Self::ActorRefT<MsgT>
    where
        MsgT: ActorMsg;

    fn set_handler<MsgT>(&self, actor_ref: &mut Self::ActorRefT<MsgT>, handler: MsgHandler<MsgT>)
    where
        MsgT: ActorMsg;

    fn spawn_async_send<MsgT>(
        &self,
        f: impl Future<Output = MsgT> + Send + 'static,
        actor_ref: impl ActorRef<MsgT> + 'static,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static;

    fn spawn_thread_blocking_send<MsgT>(
        &self,
        f: impl FnOnce() -> MsgT + Send + 'static,
        actor_ref: impl ActorRef<MsgT> + 'static,
        delay: Option<Duration>,
    ) where
        MsgT: ActorMsg + 'static;
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
