use std::{sync::mpsc, sync::mpsc::{Sender, RecvError}, thread};

use bft_grid_core::{ModuleSystem, MessageHandler, ModuleRef};
use tokio::sync::mpsc::{self as tmpsc, UnboundedSender as TUnboundedSender};

struct TokioActor<M> {
    tx: TUnboundedSender<M>
}

impl <M> ModuleRef<M> for TokioActor<M> {
    fn async_send(&mut self, message: M) -> () {
        self.tx.send(message).expect("Bug: the actor closed the receive side!");
    }
}

pub struct TokioModuleSystem {}

impl ModuleSystem for TokioModuleSystem {
 
    fn spawn<M: 'static + Send, MH: 'static + MessageHandler<M> + Send>(mut handler: MH) -> Box<dyn ModuleRef<M>> {
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
    tx: Sender<M>
}

impl <M> ModuleRef<M> for ThreadActor<M> {
    fn async_send(&mut self, message: M) -> () {
        self.tx.send(message).expect("Bug: the actor closed the receive side!");
    }
}

pub struct ThreadModuleSystem {}

impl ModuleSystem for ThreadModuleSystem {
    fn spawn<M: 'static + Send, MH: 'static + MessageHandler<M> + Send>(mut handler: MH) -> Box<dyn ModuleRef<M>> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            loop {
                match rx.recv() {
                    Err(RecvError) => {
                        break;
                    }
                    Ok(m) => {
                        handler.receive(m);
                    }
                }
            }
        });
        Box::new(ThreadActor { tx })
    }
}
