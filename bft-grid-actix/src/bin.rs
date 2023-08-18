use std::marker::PhantomData;

use actix::prelude::*;
use bft_grid_core::*;
use bft_grid_actix::*;

struct MyActor {}

struct Data;

impl ActorHandler<Data> for MyActor {
    fn receive(&mut self, _message: Data) -> () {
        ()
    }
}

#[actix::main] // <- starts the system and block until future resolves
async fn main() {
    let a = MyActor{};
    let h = ActixHandler{ handler: a, message_type: PhantomData };
    let addr = h.start();
    let res = addr.send(Msg(Data{})).await;
    print!("{:?}", res);
}
