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

pub type DynMsgHandler<MsgT> = Box<dyn TypedMsgHandler<MsgT>>;

pub type AnActorMsg = Box<dyn ActorMsg>;

impl ActorMsg for AnActorMsg {}

#[derive(Debug, Clone)]
pub struct MessageNotSupported();

impl Display for MessageNotSupported {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "Message not supported")
    }
}

impl Error for MessageNotSupported {}

/// An [`UntypedMsgHandler`] is an actor handler that can receive messages of any type,
///  although it may refuse to handle some of them.
pub trait UntypedMsgHandler: Send {
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

pub type UntypedHandlerBox = Box<dyn UntypedMsgHandler>;

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

    fn set_handler(&mut self, handler: DynMsgHandler<MsgT>);

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
    #[error("Message type not supported")]
    MessageNotSupported,
}

pub type P2PNetworkResult<R> = Result<R, P2PNetworkError>;

/// A [`P2PNetworkClient`] allows sending messages to other nodes in a P2P network.
pub trait P2PNetworkClient: DynClone {
    fn attempt_send<MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        serializer: SerializerT,
        node: impl Into<String>,
    ) -> P2PNetworkResult<()>
    where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync + 'static;
}

/// Erased versions
pub mod erased {
    use std::any::Any;
    use std::fmt::Debug;
    use std::marker::PhantomData;
    use std::{pin::Pin, time::Duration};

    use dyn_clone::DynClone;

    use crate::actor;

    use super::{ActorMsg, AnActorMsg, DynMsgHandler, P2PNetworkResult, TypedMsgHandler};

    pub type DynFuture<MsgT> = Pin<Box<dyn Future<Output = MsgT> + Send + 'static>>;
    pub type DynLazy<MsgT> = Box<dyn FnOnce() -> MsgT + Send + 'static>;

    pub trait ActorRef<MsgT>: DynClone + Send + Debug
    where
        MsgT: ActorMsg + 'static,
    {
        fn send(&mut self, message: MsgT, delay: Option<Duration>);

        fn set_handler(&mut self, handler: DynMsgHandler<MsgT>);

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

        fn set_handler(&mut self, handler: DynMsgHandler<MsgT>) {
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

        fn set_handler(&mut self, handler: DynMsgHandler<MsgT>) {
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

    #[derive(Debug)]
    pub struct DynActorRefWrapped<MsgT>
    where
        MsgT: ActorMsg,
    {
        an_actor_ref: DynActorRef<AnActorMsg>,
        _p: PhantomData<MsgT>,
    }

    impl<MsgT> Clone for DynActorRefWrapped<MsgT>
    where
        MsgT: ActorMsg,
    {
        fn clone(&self) -> Self {
            DynActorRefWrapped {
                an_actor_ref: self.an_actor_ref.clone(),
                _p: PhantomData {},
            }
        }
    }

    struct DowncastingMsgHandler<MsgT>
    where
        MsgT: ActorMsg,
    {
        dyn_msg_handler: DynMsgHandler<MsgT>,
    }

    impl<MsgT> TypedMsgHandler<AnActorMsg> for DowncastingMsgHandler<MsgT>
    where
        MsgT: ActorMsg,
    {
        fn receive(&mut self, message: AnActorMsg) -> Option<actor::ActorControl> {
            match (message as Box<dyn Any>).downcast::<MsgT>() {
                Ok(m) => self.dyn_msg_handler.receive(*m),
                Err(_) => None,
            }
        }
    }

    impl<MsgT> actor::ActorRef<MsgT> for DynActorRefWrapped<MsgT>
    where
        MsgT: ActorMsg,
    {
        fn send(&mut self, message: MsgT, delay: Option<Duration>) {
            self.an_actor_ref
                .send(Box::new(message) as Box<dyn ActorMsg>, delay);
        }

        fn set_handler(&mut self, handler: DynMsgHandler<MsgT>) {
            self.an_actor_ref
                .set_handler(Box::new(DowncastingMsgHandler::<MsgT> {
                    dyn_msg_handler: handler,
                }));
        }

        fn spawn_async_send(
            &mut self,
            f: impl Future<Output = MsgT> + Send + 'static,
            delay: Option<Duration>,
        ) {
            self.an_actor_ref.spawn_async_send(
                Box::pin(async move { Box::new(f.await) as Box<dyn ActorMsg> }),
                delay,
            );
        }

        fn spawn_thread_blocking_send(
            &mut self,
            f: impl FnOnce() -> MsgT + Send + 'static,
            delay: Option<Duration>,
        ) {
            self.an_actor_ref
                .spawn_thread_blocking_send(Box::new(|| Box::new(f())), delay);
        }
    }

    impl actor::ActorSystemHandle for dyn ActorSystemHandle {
        type ActorRefT<MsgT>
            = DynActorRefWrapped<MsgT>
        where
            MsgT: ActorMsg;

        fn create<MsgT>(
            &self,
            node_id: impl Into<String>,
            name: impl Into<String>,
            join_on_drop: bool,
        ) -> Self::ActorRefT<MsgT>
        where
            MsgT: ActorMsg,
        {
            DynActorRefWrapped {
                an_actor_ref: ActorSystemHandle::create(
                    self,
                    node_id.into(),
                    name.into(),
                    join_on_drop,
                ),
                _p: PhantomData {},
            }
        }
    }

    pub trait P2PNetworkClient: DynClone + Send {
        fn attempt_send(
            &mut self,
            message: AnActorMsg,
            serializer: Box<dyn Fn(AnActorMsg) -> P2PNetworkResult<Vec<u8>> + Sync>,
            node: String,
        ) -> P2PNetworkResult<()>;
    }

    pub type DynP2PNetworkClient = Box<dyn P2PNetworkClient>;

    impl Clone for DynP2PNetworkClient {
        fn clone(&self) -> Self {
            dyn_clone::clone_box(&**self)
        }
    }

    impl<P2PNetworkClientT> P2PNetworkClient for P2PNetworkClientT
    where
        P2PNetworkClientT: actor::P2PNetworkClient + Send + ?Sized + 'static,
    {
        fn attempt_send(
            &mut self,
            message: AnActorMsg,
            serializer: Box<dyn Fn(AnActorMsg) -> P2PNetworkResult<Vec<u8>> + Sync>,
            node: String,
        ) -> P2PNetworkResult<()> {
            self.attempt_send(message, serializer, node)
        }
    }

    impl actor::P2PNetworkClient for dyn P2PNetworkClient {
        fn attempt_send<MsgT, SerializerT>(
            &mut self,
            message: MsgT,
            serializer: SerializerT,
            node: impl Into<String>,
        ) -> P2PNetworkResult<()>
        where
            MsgT: ActorMsg,
            SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync + 'static,
        {
            P2PNetworkClient::attempt_send(
                self,
                Box::new(message),
                Box::new(move |msg| match (msg as Box<dyn Any>).downcast::<MsgT>() {
                    Ok(typed_msg) => serializer(*typed_msg),
                    Err(_) => Err(actor::P2PNetworkError::MessageNotSupported),
                }),
                node.into(),
            )
        }
    }
}
