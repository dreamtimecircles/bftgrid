use std::{
    error::Error,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    io,
    sync::Arc,
    time::Duration,
};

use downcast_rs::{Downcast, impl_downcast};
use dyn_clone::{DynClone, clone_trait_object};
use thiserror::Error;
use tokio::task::JoinError;

pub trait ActorMsg: DynClone + Downcast + Send + Debug {}
clone_trait_object!(ActorMsg);
impl_downcast!(ActorMsg);

pub enum ActorControl {
    Exit(),
}

pub trait TypedHandler: Send + Debug {
    type MsgT: ActorMsg;

    fn receive(&mut self, message: Self::MsgT) -> Option<ActorControl>;
}

#[derive(Debug, Clone)]
pub struct MessageNotSupported();

// Errors should be printable.
impl Display for MessageNotSupported {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "Message not supported")
    }
}

impl Error for MessageNotSupported {}

pub type AnActorMsg = Box<dyn ActorMsg>;

pub trait UntypedHandler: Send + Debug {
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

pub type UntypedHandlerBox = Box<dyn UntypedHandler>;

impl<MsgT, HandlerT> UntypedHandler for HandlerT
where
    MsgT: ActorMsg,
    HandlerT: TypedHandler<MsgT = MsgT>,
{
    fn receive_untyped(
        &mut self,
        message: AnActorMsg,
    ) -> Result<Option<ActorControl>, MessageNotSupported> {
        match message.downcast::<HandlerT::MsgT>() {
            Ok(typed_message) => Result::Ok(self.receive(*typed_message)),
            Err(_) => Result::Err(MessageNotSupported()),
        }
    }
}

pub trait Joinable<Output>: Send + Debug {
    fn join(self) -> Output;
    fn is_finished(&mut self) -> bool;
}

pub type AnActorRef<MsgT, HandlerT> = Box<dyn ActorRef<MsgT, HandlerT>>;

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
}

#[derive(Error, Debug, Clone)]
pub enum P2PNetworkError {
    #[error("I/O error")]
    Io(#[from] Arc<io::Error>),
    #[error("Join error")]
    Join(#[from] Arc<JoinError>),
    #[error("Actor not found")]
    ActorNotFound(String),
}

pub type P2PNetworkResult<R> = Result<R, P2PNetworkError>;

pub trait P2PNetwork: Clone {
    fn attempt_send<MsgT, SerializerT>(
        &mut self,
        message: MsgT,
        serializer: &SerializerT,
        node: impl AsRef<str>,
    ) -> P2PNetworkResult<()>
    where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT) -> P2PNetworkResult<Vec<u8>> + Sync;
}
