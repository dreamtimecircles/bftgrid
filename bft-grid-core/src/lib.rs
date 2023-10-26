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
    fn join(self) -> Output;
    fn is_finished(&mut self) -> bool;
}

pub trait SingleThreadedActorRef<Msg>
where
    Msg: 'static,
{
    fn send(&mut self, message: Msg, delay: Option<Duration>) -> Box<dyn Joinable<Option<()>>>;
}

pub trait ActorRef<Msg, MessageHandler>: SingleThreadedActorRef<Msg> + Joinable<()> + Send
where
    Msg: 'static,
    MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static,
{
    fn clone(&self) -> Box<dyn ActorRef<Msg, MessageHandler>>;
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
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static;
}

pub trait ActorSystem {
    type ConcreteActorRef<Msg, MessageHandler>: SingleThreadedActorRef<Msg>
    where
        Msg: Send + 'static,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static;

    fn create<Msg, MessageHandler>(
        &mut self,
        node: String,
        actor_name: String,
    ) -> Self::ConcreteActorRef<Msg, MessageHandler>
    where
        Msg: Send + 'static,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + Send + 'static;

    fn set_handler<Msg, MessageHandler: TypedMessageHandler<'static, Msg = Msg> + 'static>(
        &mut self,
        actor_ref: &mut Self::ConcreteActorRef<Msg, MessageHandler>,
        handler: MessageHandler,
    ) where
        Msg: 'static + Send,
        MessageHandler: TypedMessageHandler<'static, Msg = Msg> + Send + 'static;
}

pub trait P2PNetwork {
    fn send<Msg: 'static>(&mut self, message: Msg, node: String);
    fn broadcast<Msg: Clone + 'static>(&mut self, message: Msg);
}
