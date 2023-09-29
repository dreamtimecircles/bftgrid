use std::{
    sync::mpsc,
    sync::mpsc::{RecvError, Sender},
    thread,
};

use bft_grid_core::{ActorRef, ActorSystem, TypedMessageHandler};
use tokio::sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender};

struct TokioActor<M> {
    tx: TUnboundedSender<M>,
}

impl<M> ActorRef<M> for TokioActor<M> {
    fn async_send(&mut self, message: M) -> () {
        self.tx
            .send(message)
            .expect("Bug: the actor closed the receive side!");
    }
}

pub struct TokioActorSystem {}

impl<'this, 'actor_name> ActorSystem<'this, 'actor_name> for TokioActorSystem {
    fn spawn_actor<
        Msg: 'static + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &'this mut self,
        _name: &'actor_name str,
        mut handler: MH,
    ) -> Box<dyn ActorRef<Msg> + 'this> {
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

struct ThreadActor<M> {
    tx: Sender<M>,
}

impl<M> ActorRef<M> for ThreadActor<M> {
    fn async_send(&mut self, message: M) -> () {
        self.tx
            .send(message)
            .expect("Bug: the actor closed the receive side!");
    }
}

pub struct ThreadActorSystem {}

impl<'this, 'actor_name> ActorSystem<'this, 'actor_name> for ThreadActorSystem {
    fn spawn_actor<
        Msg: 'static + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &'this mut self,
        _name: &'actor_name str,
        mut handler: MH,
    ) -> Box<dyn ActorRef<Msg> + 'this> {
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
