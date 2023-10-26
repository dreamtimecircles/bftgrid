use std::{
    any::Any,
    error::Error,
    fmt::{Display, Error as FmtError, Formatter},
    time::Duration,
};

pub enum ActorControl {
    Exit(),
}

pub trait TypedMessageHandler<'msg> {
    type Msg: 'msg;

    fn receive(&mut self, message: Self::Msg) -> Option<ActorControl>;
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

pub trait UntypedMessageHandler<'msg> {
    fn receive_untyped(
        &mut self,
        message: Box<dyn Any + 'msg>,
    ) -> Result<Option<ActorControl>, MessageNotSupported>;
}

impl<'msg, Msg, X> UntypedMessageHandler<'msg> for X
where
    Msg: 'msg,
    X: TypedMessageHandler<'msg, Msg = Msg>,
{
    fn receive_untyped(
        &mut self,
        message: Box<dyn Any + 'msg>,
    ) -> Result<Option<ActorControl>, MessageNotSupported> {
        match message.downcast::<X::Msg>() {
            Ok(typed_message) => Result::Ok(self.receive(*typed_message)),
            Err(_) => Result::Err(MessageNotSupported()),
        }
    }
}

pub trait Joinable<Output> {
    fn join(self: Box<Self>) -> Output;
    fn is_finished(&mut self) -> bool;
}

pub trait SingleThreadedActorRef<Msg> {
    fn send(&mut self, message: Msg, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>>;
}

pub trait ActorRef<Msg>: SingleThreadedActorRef<Msg> + Joinable<()> + Send {
    fn clone(&self) -> Box<dyn ActorRef<Msg>>;
}

pub trait SingleThreadedActorSystem {
    fn spawn_actor<Msg, MessageHandler: 'static>(
        &mut self,
        node: String,
        actor_name: String,
        handler: MessageHandler,
    ) -> Box<dyn SingleThreadedActorRef<Msg>>
    where
        Msg: 'static,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg>;
}

pub trait ActorSystem {
    fn spawn_actor<Msg, MessageHandler: 'static>(
        &mut self,
        node: String,
        actor_name: String,
        handler: MessageHandler,
    ) -> Box<dyn ActorRef<Msg>>
    where
        Msg: 'static + Send,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg> + Send;
}

pub trait P2PNetwork {
    fn send<Msg: 'static>(&mut self, message: Msg, node: String);
    fn broadcast<Msg: Clone + 'static>(&mut self, message: Msg);
}
