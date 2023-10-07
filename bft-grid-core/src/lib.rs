use std::{
    any::Any,
    error::Error,
    fmt::{Display, Error as FmtError, Formatter},
};

pub trait TypedMessageHandler<'msg> {
    type Msg: 'msg;

    fn receive(&mut self, message: Self::Msg);
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
    fn receive_untyped(&mut self, message: Box<dyn Any + 'msg>) -> Result<(), MessageNotSupported>;
}

impl<'msg, Msg: 'msg, X: TypedMessageHandler<'msg, Msg = Msg>> UntypedMessageHandler<'msg> for X {
    fn receive_untyped(&mut self, message: Box<dyn Any + 'msg>) -> Result<(), MessageNotSupported> {
        match message.downcast::<X::Msg>() {
            Ok(typed_message) => {
                self.receive(*typed_message);
                Result::Ok(())
            }
            Err(_) => Result::Err(MessageNotSupported()),
        }
    }
}

pub trait ActorRef<Msg>: Send {
    fn async_send(&self, message: Msg);
}

pub trait ActorSystem {
    fn spawn_actor<
        Msg: 'static + Sync + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &mut self,
        name: String,
        handler: MH,
    ) -> Box<dyn ActorRef<Msg> + Send>;
}
