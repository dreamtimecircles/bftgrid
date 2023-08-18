use std::marker::PhantomData;

use actix::{prelude::*, dev::ToEnvelope};
use bft_grid_core::*;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Msg<M: Unpin + 'static> (pub M);

struct ActixModule<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    handler: H,
    message_type: PhantomData<Msg<M>>,
}

pub struct AsyncActixModule<M: Unpin + 'static, H: MessageHandler<M> + Unpin + 'static>
(
    ActixModule<M, H>,
);

pub struct SyncActixModule<M: Unpin + 'static, H: MessageHandler<M> + Unpin + 'static>
(
    ActixModule<M, H>,
);

impl <M, H> Actor for AsyncActixModule<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl <M, H> Actor for SyncActixModule<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    type Context = SyncContext<Self>;
}

impl <M, H> Handler<Msg<M>> for AsyncActixModule<M, H,>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: Msg<M>, _ctx: &mut Self::Context) -> Self::Result {
        self.0.handler.receive(msg.0)
    }
}

impl <M, H> Handler<Msg<M>> for SyncActixModule<M, H,>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: Msg<M>, _ctx: &mut Self::Context) -> Self::Result {
        receive(&mut self.0.handler, msg)
    }
}

fn receive<M, H>(h: &mut H, msg: Msg<M>) -> ()
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    h.receive(msg.0)
}

impl <M, H> AsyncActixModule<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    pub fn new(h: H) -> Self {
        AsyncActixModule(ActixModule { handler: h, message_type: PhantomData })
    }
}

impl <M, H> SyncActixModule<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    pub fn new(h: H) -> Self {
        SyncActixModule(ActixModule { handler: h, message_type: PhantomData })
    }
}

pub struct AsyncActixModuleRef<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    addr: Addr<AsyncActixModule<M, H>>
}

pub struct SyncActixModuleRef<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    addr: Addr<SyncActixModule<M, H>>
}

impl <M, H> ModuleRef<M> for AsyncActixModuleRef<M, H>
    where
        M: Unpin + Send,
        H: MessageHandler<M> + Unpin + 'static,
{
    fn async_send(&mut self, message: M) -> () {
        send(&mut self.addr, message)
    }
}

impl <M, H> ModuleRef<M> for SyncActixModuleRef<M, H>
    where
        M: Unpin + Send,
        H: MessageHandler<M> + Unpin + 'static,
{
    fn async_send(&mut self, message: M) -> () {
        send(&mut self.addr, message)
    }
}

fn send<A, M>(addr: &mut Addr<A>, message: M) -> ()
    where
        A: Actor + Handler<Msg<M>>,
        <A as Actor>::Context: ToEnvelope<A, Msg<M>>,
        M: Unpin + Send + 'static,
{
    let _ = addr.send(Msg(message));
}

impl <M, H> AsyncActixModuleRef<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    pub fn new(addr: Addr<AsyncActixModule<M, H>>) -> Self {
        AsyncActixModuleRef { addr }
    }
}

impl <M, H> SyncActixModuleRef<M, H>
    where
        M: Unpin + 'static,
        H: MessageHandler<M> + Unpin + 'static,
{
    pub fn new(addr: Addr<SyncActixModule<M, H>>) -> Self {
        SyncActixModuleRef { addr }
    }
}
