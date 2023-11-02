use std::{
    error::Error,
    fmt::{Debug, Display, Error as FmtError, Formatter},
    time::Duration,
};

use downcast_rs::{Downcast, impl_downcast};
use dyn_clone::{DynClone, clone_trait_object};

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

pub trait Joinable<Output> {
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

impl <MsgT, HandlerT> Clone for Box<dyn ActorRef<MsgT, HandlerT>>
where
    MsgT: ActorMsg + 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static
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

pub trait P2PNetwork {
    fn send<MsgT>(&mut self, message: MsgT, node: String)
    where
        MsgT: ActorMsg + 'static;

    fn broadcast<MsgT>(&mut self, message: MsgT)
    where
        MsgT: ActorMsg + 'static;
}
