use std::{any::Any, borrow::Cow};

pub trait TypedMessageHandler<'msg> {
    type Msg: 'msg;

    fn receive(&mut self, message: Self::Msg) -> ();
}

pub trait UntypedMessageHandler<'msg> {
    fn receive_untyped(&mut self, message: Box<dyn Any + 'msg>) -> Result<(), ()>;
}

impl<'msg, Msg: 'msg, X: TypedMessageHandler<'msg, Msg = Msg>> UntypedMessageHandler<'msg> for X {
    fn receive_untyped(&mut self, message: Box<dyn Any + 'msg>) -> Result<(), ()> {
        match message.downcast::<X::Msg>() {
            Ok(typed_message) => {
                self.receive(*typed_message);
                Result::Ok(())
            }
            Err(_) => Result::Err(()),
        }
    }
}

pub trait ActorRef<Msg> {
    fn async_send(&mut self, message: Msg) -> ();
}

pub type ActorName = Cow<'static, str>;

pub trait ActorSystem {
    fn spawn_actor<
        Msg: 'static + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &mut self,
        name: ActorName,
        handler: MH,
    ) -> Box<dyn ActorRef<Msg>>;
}
