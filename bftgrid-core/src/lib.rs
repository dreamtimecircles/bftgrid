use std::{
    any::Any,
    error::Error,
    fmt::{Display, Error as FmtError, Formatter},
    time::Duration,
};

pub enum ActorControl {
    Exit(),
}

pub trait TypedHandler<'msg> {
    type MsgT: 'msg;

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

pub trait UntypedHandler<'msg> {
    fn receive_untyped(
        &mut self,
        message: Box<dyn Any + 'msg>,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

impl<'msg, MsgT, HandlerT> UntypedHandler<'msg> for HandlerT
where
    MsgT: 'msg,
    HandlerT: TypedHandler<'msg, MsgT = MsgT>,
{
    fn receive_untyped(
        &mut self,
        message: Box<dyn Any + 'msg>,
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

pub trait ActorRef<MsgT, HandlerT>: Joinable<()> + Send
where
    MsgT: 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>>;
    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>>;
}

pub trait ActorSystem: Clone {
    type ActorRefT<MsgT, HandlerT>: ActorRef<MsgT, HandlerT>
    where
        MsgT: Send + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static;

    fn create<MsgT, HandlerT>(
        &mut self,
        node_id: String,
        name: String,
    ) -> Self::ActorRefT<MsgT, HandlerT>
    where
        MsgT: Send + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static;

    fn set_handler<MsgT, HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static>(
        &mut self,
        actor_ref: &mut Self::ActorRefT<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: Send + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static;
}

pub trait P2PNetwork {
    fn send<MsgT>(&mut self, message: MsgT, node: String)
    where
        MsgT: Send + 'static;

    fn broadcast<MsgT>(&mut self, message: MsgT)
    where
        MsgT: Clone + Send + 'static;
}
