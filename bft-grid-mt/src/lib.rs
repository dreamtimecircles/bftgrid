use std::{
    sync::mpsc,
    sync::mpsc::{RecvError, Sender},
    thread,
};

use bft_grid_core::{ActorRef, ActorSystem, TypedMessageHandler};
use tokio::sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender};

struct TokioActor<M: Send> {
    tx: TUnboundedSender<M>,
}

impl<M: Send> ActorRef<M> for TokioActor<M> {
    fn async_send(&self, message: M) {
        self.tx
            .send(message)
            .expect("Bug: the actor closed the receive side!");
    }
}

pub struct TokioActorSystem {}

impl ActorSystem for TokioActorSystem {
    fn spawn_actor<
        Msg: 'static + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &mut self,
        _name: String,
        mut handler: MH,
    ) -> Box<dyn ActorRef<Msg>> {
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

struct ThreadActor<M: Send> {
    tx: Sender<M>,
}

impl<M: Send> ActorRef<M> for ThreadActor<M> {
    fn async_send(&self, message: M) {
        self.tx
            .send(message)
            .expect("Bug: the actor closed the receive side!");
    }
}

pub struct ThreadActorSystem {}

impl ActorSystem for ThreadActorSystem {
    fn spawn_actor<
        Msg: 'static + Sync + Send,
        MH: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    >(
        &mut self,
        _name: String,
        mut handler: MH,
    ) -> Box<dyn ActorRef<Msg>> {
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
