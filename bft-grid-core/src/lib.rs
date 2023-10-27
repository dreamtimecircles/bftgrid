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

impl<'msg, Msg, HandlerT> UntypedHandler<'msg> for HandlerT
where
    Msg: 'msg,
    HandlerT: TypedHandler<'msg, MsgT = Msg>,
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

pub trait SingleThreadedActorRef<MsgT>
where
    MsgT: 'static,
{
    fn send(&mut self, message: MsgT, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>>;
}

pub trait ActorRef<MsgT, HandlerT>: SingleThreadedActorRef<MsgT> + Joinable<()> + Send
where
    MsgT: 'static,
    HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static,
{
    fn new_ref(&self) -> Box<dyn ActorRef<MsgT, HandlerT>>;
}

pub trait SimulatedActorSystem {
    fn spawn_actor<MsgT, HandlerT: 'static>(
        &mut self,
        node: String,
        actor_name: String,
        handler: HandlerT,
    ) -> Box<dyn SingleThreadedActorRef<MsgT>>
    where
        MsgT: 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;
}

pub trait ActorSystem {
    type ConcreteActorRef<MsgT, HandlerT>: SingleThreadedActorRef<MsgT>
    where
        MsgT: Send + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static;

    fn create<MsgT, HandlerT>(&mut self) -> Self::ConcreteActorRef<MsgT, HandlerT>
    where
        MsgT: Send + 'static,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static;

    fn set_handler<MsgT, HandlerT: TypedHandler<'static, MsgT = MsgT> + 'static>(
        &mut self,
        actor_ref: &mut Self::ConcreteActorRef<MsgT, HandlerT>,
        handler: HandlerT,
    ) where
        MsgT: 'static + Send,
        HandlerT: TypedHandler<'static, MsgT = MsgT> + Send + 'static;
}

pub trait P2PNetwork {
    fn send<MsgT: 'static>(&mut self, message: MsgT, node: String);
    fn broadcast<MsgT: Clone + 'static>(&mut self, message: MsgT);
}
