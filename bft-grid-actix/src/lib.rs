use std::marker::PhantomData;

use actix::prelude::*;
use bft_grid_core::ActorHandler;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Msg<M: Unpin + 'static> (pub M);

pub struct ActixHandler<M, H>
    where
        M: Unpin + 'static,
        H: ActorHandler<M> + Unpin + 'static,
{
    pub handler: H,
    pub message_type: PhantomData<Msg<M>>,
}

impl <M, H> Actor for ActixHandler<M, H>
    where
        M: Unpin,
        H: ActorHandler<M> + Unpin + 'static,
{
    type Context = Context<Self>;
}


impl <M, H> Handler<Msg<M>> for ActixHandler<M, H>
    where
        M: Unpin + 'static,
        H: ActorHandler<M> + Unpin + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: Msg<M>, _ctx: &mut Self::Context) -> Self::Result {
        self.handler.receive(msg.0)
    }
}
