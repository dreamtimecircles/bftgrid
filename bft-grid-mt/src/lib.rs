use std::{
    sync::mpsc,
    sync::{
        mpsc::{RecvError, Sender},
        Arc,
    },
    thread,
    time::Duration,
};

use async_trait::async_trait;
use bft_grid_core::{ActorRef, ActorSystem, SingleThreadedActorRef, TypedMessageHandler};
use tokio::{
    runtime::Runtime,
    sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender},
};

struct TokioActor<M>
where
    M: Send,
{
    runtime: Arc<Runtime>,
    tx: TUnboundedSender<M>,
}

#[async_trait]
impl<Msg> SingleThreadedActorRef<Msg> for TokioActor<Msg>
where
    Msg: Send + 'static,
{
    fn send(&mut self, message: Msg, delay: Option<Duration>) {
        let sender = self.tx.clone();
        self.runtime.spawn(async move {
            if let Some(delay_duration) = delay {
                tokio::time::sleep(delay_duration).await;
            }
            sender
                .send(message)
                .expect("Bug: the actor closed the receive side!");
        });
    }
}

impl<Msg> ActorRef<Msg> for TokioActor<Msg> where Msg: Send + 'static {}

pub struct TokioActorSystem {
    runtime: Arc<Runtime>,
}

impl TokioActorSystem {
    pub fn new() -> Self {
        TokioActorSystem {
            runtime: Arc::new(Runtime::new().unwrap()),
        }
    }
}

impl Default for TokioActorSystem {
    fn default() -> Self {
        TokioActorSystem::new()
    }
}

impl ActorSystem for TokioActorSystem {
    fn spawn_actor<Msg, MessageHandler>(
        &mut self,
        _node_name: String,
        _actor_name: String,
        mut handler: MessageHandler,
    ) -> Box<dyn ActorRef<Msg>>
    where
        Msg: 'static + Send,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
    {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        self.runtime.spawn(async move {
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
        Box::new(TokioActor {
            runtime: self.runtime.clone(),
            tx,
        })
    }
}

struct ThreadActor<Msg>
where
    Msg: Send,
{
    tx: Sender<Msg>,
}

#[async_trait]
impl<Msg> SingleThreadedActorRef<Msg> for ThreadActor<Msg>
where
    Msg: Send,
{
    fn send(&mut self, message: Msg, delay: Option<Duration>) {
        if let Some(delay_duration) = delay {
            thread::sleep(delay_duration);
        }
        self.tx
            .send(message)
            .expect("Bug: the actor closed the receive side!");
    }
}

impl<Msg> ActorRef<Msg> for ThreadActor<Msg> where Msg: Send {}

pub struct ThreadActorSystem {}

impl ActorSystem for ThreadActorSystem {
    fn spawn_actor<Msg, MessageHandler: 'static>(
        &mut self,
        _node_name: String,
        _actor_name: String,
        mut handler: MessageHandler,
    ) -> Box<dyn ActorRef<Msg>>
    where
        Msg: 'static + Send,
        MessageHandler: 'static + TypedMessageHandler<'static, Msg = Msg> + Send,
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
