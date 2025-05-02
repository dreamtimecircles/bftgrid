//! An actor framework geared towards BFT ordering libraries with support for deterministic simulation testing.
//! It also allows writing a generic overall system construction logic independent from the specific actor system used.
//! Every actor is managed by a single actor system but can interact with actors managed by other actor systems.
//!
//! No async signatures are used in the public API in order to support single-threaded simulation testing without relying on async runtimes.
//! Actors should not assume anything about the thread they are running on, nor use any async runtime. They must rely on special functions
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
pub trait TypedMsgHandler<MsgT>: Send
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

impl ActorMsg for AnActorMsg {}

/// An [`UntypedMsgHandler`] is an actor handler that can receive messages of any type,
///  although it may refuse to handle some of them.
pub trait UntypedMsgHandler: Send {
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
        match (message.clone() as Box<dyn Any>).downcast::<MsgT>() {
            Ok(typed_message) => Result::Ok(self.receive(*typed_message)),
            Err(_) => {
                // MsgT may be a trait object, so we retry after boxing the message
                match (Box::new(message) as Box<dyn Any>).downcast::<MsgT>() {
                    Ok(typed_message) => Result::Ok(self.receive(*typed_message)),
                    Err(_) => Result::Err(MessageNotSupported()),
                }
            }
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

/// An [`ActorRef`] can asynchronously send messages to the underlying actor, optionally with a delay,
///  change the actor handler, spawn an async self-send, spawn a thread-blocking self-send and it can be cloned.
/// Actor implementations can use [`ActorRef`]s to send messages to themselves and to other actors.
pub trait ActorRef<MsgT>: DynClone + Send + Debug
where
    MsgT: ActorMsg + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>);

    fn set_handler(&mut self, handler: MsgHandler<MsgT>);

    fn spawn_async_send(
        &mut self,
        f: impl Future<Output = MsgT> + Send + 'static,
        delay: Option<Duration>,
    );

    fn spawn_thread_blocking_send(
        &mut self,
        f: impl FnOnce() -> MsgT + Send + 'static,
        delay: Option<Duration>,
    );
}

/// An [`ActorSystemHandle`] allows spawning actors by creating an [`ActorRef`] and it can be cloned.
/// Actors themselves can use [`ActorSystemHandle`]s to spawn new actors.
pub trait ActorSystemHandle: DynClone + Send {
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

/// Erased versions
pub mod erased {
    use std::fmt::Debug;
    use std::{pin::Pin, time::Duration};

    use dyn_clone::DynClone;

    use crate::actor;

    use super::{ActorMsg, AnActorMsg, MsgHandler};

    pub type DynFuture<MsgT> = Pin<Box<dyn Future<Output = MsgT> + Send + 'static>>;
    pub type DynLazy<MsgT> = Box<dyn FnOnce() -> MsgT + Send + 'static>;

    pub trait ActorRef<MsgT>: DynClone + Send + Debug
    where
        MsgT: ActorMsg + 'static,
    {
        fn send(&mut self, message: MsgT, delay: Option<Duration>);

        fn set_handler(&mut self, handler: MsgHandler<MsgT>);

        fn spawn_async_send(&mut self, f: DynFuture<MsgT>, delay: Option<Duration>);

        fn spawn_thread_blocking_send(&mut self, f: DynLazy<MsgT>, delay: Option<Duration>);
    }

    pub type DynActorRef<MsgT> = Box<dyn ActorRef<MsgT>>;

    impl<MsgT> Clone for DynActorRef<MsgT> {
        fn clone(&self) -> Self {
            dyn_clone::clone_box(&**self)
        }
    }

    impl<ActorRefT, MsgT> ActorRef<MsgT> for ActorRefT
    where
        ActorRefT: actor::ActorRef<MsgT> + ?Sized,
        MsgT: ActorMsg + 'static,
    {
        fn send(&mut self, message: MsgT, delay: Option<Duration>) {
            self.send(message, delay);
        }

        fn set_handler(&mut self, handler: MsgHandler<MsgT>) {
            self.set_handler(handler);
        }

        fn spawn_async_send(&mut self, f: DynFuture<MsgT>, delay: Option<Duration>) {
            self.spawn_async_send(f, delay);
        }

        fn spawn_thread_blocking_send(&mut self, f: DynLazy<MsgT>, delay: Option<Duration>) {
            self.spawn_thread_blocking_send(f, delay);
        }
    }

    impl<MsgT> actor::ActorRef<MsgT> for dyn ActorRef<MsgT>
    where
        MsgT: ActorMsg + 'static,
    {
        fn send(&mut self, message: MsgT, delay: Option<Duration>) {
            ActorRef::send(self, message, delay);
        }

        fn set_handler(&mut self, handler: MsgHandler<MsgT>) {
            ActorRef::set_handler(self, handler);
        }

        fn spawn_async_send(
            &mut self,
            f: impl Future<Output = MsgT> + Send + 'static,
            delay: Option<Duration>,
        ) {
            ActorRef::spawn_async_send(self, Box::pin(f), delay);
        }

        fn spawn_thread_blocking_send(
            &mut self,
            f: impl FnOnce() -> MsgT + Send + 'static,
            delay: Option<Duration>,
        ) {
            ActorRef::spawn_thread_blocking_send(self, Box::new(f), delay);
        }
    }

    pub trait ActorSystemHandle: DynClone + Send {
        fn create(
            &self,
            node_id: String,
            name: String,
            join_on_drop: bool,
        ) -> DynActorRef<AnActorMsg>;
    }

    pub type DynActorSystemHandle = Box<dyn ActorSystemHandle>;

    impl Clone for DynActorSystemHandle {
        fn clone(&self) -> Self {
            dyn_clone::clone_box(&**self)
        }
    }

    impl<ActorSystemHandleT> ActorSystemHandle for ActorSystemHandleT
    where
        ActorSystemHandleT: actor::ActorSystemHandle + ?Sized + 'static,
    {
        fn create(
            &self,
            node_id: String,
            name: String,
            join_on_drop: bool,
        ) -> DynActorRef<AnActorMsg> {
            Box::new(self.create(node_id, name, join_on_drop))
        }
    }
}
