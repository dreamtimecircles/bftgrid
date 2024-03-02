use std::{
    error::Error,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{clone_trait_object, DynClone};

pub trait ActorMsg: DynClone + Downcast + Send + Debug {}
clone_trait_object!(ActorMsg);
impl_downcast!(ActorMsg);

pub enum ActorControl {
    Exit(),
}

pub trait TypedHandler<'msg>: Send + Debug {
    type MsgT: ActorMsg + 'msg;

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

pub trait UntypedHandler<'msg>: Send + Debug {
    fn receive_untyped(
        &mut self,
        message: Box<dyn ActorMsg + 'msg>,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

pub type UntypedHandlerBox = Box<dyn UntypedHandler<'static>>;

impl<'msg, MsgT, HandlerT> UntypedHandler<'msg> for HandlerT
where
    MsgT: ActorMsg + 'msg,
    HandlerT: TypedHandler<'msg, MsgT = MsgT>,
{
    fn receive_untyped(
        &mut self,
        message: Box<dyn ActorMsg + 'msg>,
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

pub trait ActorRef<MsgT, HandlerT>: Send + Debug
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>);
    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>>;
}

impl<MsgT, HandlerT> Clone for Box<dyn ActorRef<MsgT, HandlerT>>
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn clone(&self) -> Self {
        self.new_ref()
    }
}

pub trait ActorSystem: Clone + Send + Debug {
    type ActorRefT<MsgT, HandlerT>: ActorRef<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        node_id: String,
        name: String,
    ) -> Self::ActorRefT<MsgT, HandlerT>
    where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;

    fn set_handler<MsgT, HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: ActorMsg + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;
}

#[async_trait]
pub trait P2PNetwork {
    async fn send<'s, MsgT, SerializerT, const BUFFER_SIZE: usize>(
        &mut self,
        message: MsgT,
        serializer: &'s SerializerT,
        node: Arc<String>,
    ) where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT, &mut [u8]) -> anyhow::Result<usize> + Sync;

    async fn broadcast<'s, MsgT, SerializerT, const BUFFER_SIZE: usize>(
        &mut self,
        message: MsgT,
        serializer: &'s SerializerT,
    ) where
        MsgT: ActorMsg,
        SerializerT: Fn(MsgT, &mut [u8]) -> anyhow::Result<usize> + Sync;
}
