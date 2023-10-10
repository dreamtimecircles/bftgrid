use std::{
    sync::mpsc,
    sync::mpsc::{RecvError, Sender},
    thread,
};

use bft_grid_core::{ActorRef, ActorSystem, SingleThreadedActorRef, TypedMessageHandler};
use tokio::sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender};

struct TokioActor<M>
where
    M: Send,
{
    tx: TUnboundedSender<M>,
}

impl<M> SingleThreadedActorRef<M> for TokioActor<M>
where
    M: Send,
{
    fn async_send(&self, message: M) {
        self.tx
            .send(message)
            .expect("Bug: the actor closed the receive side!");
    }
}

impl<M> ActorRef<M> for TokioActor<M> where M: Send {}

pub struct TokioActorSystem {}

impl ActorSystem for TokioActorSystem {
    fn spawn_actor<Msg, MessageHandler>(
        &mut self,
        _name: String,
        mut handler: MessageHandler,
    ) -> Box<dyn ActorRef<Msg>>
    where
        Msg: 'static + Send,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    None => {
                        rx.close();
                        break;
                    }
                    Some(m) => {
                        handler.receive(m);
                    }
                }
            }
        });
        Box::new(TokioActor { tx })
    }
}

struct ThreadActor<M>
where
    M: Send,
{
    tx: Sender<M>,
}

impl<M> SingleThreadedActorRef<M> for ThreadActor<M>
where
    M: Send,
{
    fn async_send(&self, message: M) {
        self.tx
            .send(message)
            .expect("Bug: the actor closed the receive side!");
    }
}

impl<M> ActorRef<M> for ThreadActor<M> where M: Send {}

pub struct ThreadActorSystem {}

impl ActorSystem for ThreadActorSystem {
    fn spawn_actor<Msg, MH>(&mut self, _name: String, mut handler: MH) -> Box<dyn ActorRef<Msg>>
    where
        Msg: 'static + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || loop {
            match rx.recv() {
                Err(RecvError) => {
                    break;
                }
                Ok(m) => {
                    handler.receive(m);
                }
            }
        });
        Box::new(ThreadActor { tx })
    }
}
