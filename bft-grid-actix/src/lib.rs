use std::marker::PhantomData;

use actix::{prelude::*, dev::ToEnvelope};
use bft_grid_core::*;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Msg<M: Unpin + 'static> (pub M);

pub struct ActixModule<M, H, const ASYNC: bool>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    handler: H,
    message_type: PhantomData<Msg<M>>,
}

impl <M, H> Actor for ActixModule<M, H, true>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl <M, H> Actor for ActixModule<M, H, false>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    type Context = SyncContext<Self>;
}

impl <M, H, const ASYNC: bool> Handler<Msg<M>> for ActixModule<M, H, ASYNC>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
        Self: Actor,
{
    type Result = ();

    fn handle(&mut self, msg: Msg<M>, _ctx: &mut Self::Context) -> Self::Result {
        self.handler.receive(msg.0)
    }
}

impl <M, H, const ASYNC: bool> ActixModule<M, H, ASYNC>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    pub fn new(h: H) -> Self {
        ActixModule{ handler: h, message_type: PhantomData }
    }
}

pub struct ActixModuleRef<M, H, const ASYNC: bool>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
        ActixModule<M, H, ASYNC>: Actor,
{
    addr: Addr<ActixModule<M, H, ASYNC>>
}

impl <M, H, const ASYNC: bool> ModuleRef<M> for ActixModuleRef<M, H, ASYNC>
    where
        M: Unpin + Send,
        H: MessageHandler<M> + Unpin + 'static,
        ActixModule<M, H, ASYNC>: Actor,
        <ActixModule<M, H, ASYNC> as actix::Actor>::Context: ToEnvelope<ActixModule<M, H, ASYNC>, Msg<M>>,
{
    fn async_send(&mut self, message: M) -> () {
        let _ = self.addr.send(Msg(message));
    }
}

impl <M, H, const ASYNC: bool> ActixModuleRef<M, H, ASYNC>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
        ActixModule<M, H, ASYNC>: Actor,
{
    pub fn new(addr: Addr<ActixModule<M, H, ASYNC>>) -> Self {
        ActixModuleRef { addr }
    }
}
