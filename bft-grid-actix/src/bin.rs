
use std::time::Duration;

use actix::prelude::*;
use bft_grid_core::*;
use bft_grid_actix::*;

#[derive(Debug)]
struct Data;

struct MyModule<R: ModuleRef<Data>> {
    reply_to: Option<R>
}

impl <R: ModuleRef<Data>> MessageHandler<Data> for MyModule<R> {
    fn receive(&mut self, message: Data) -> () {
        println!("Received {:?}", message);
        match &mut self.reply_to {
            Some(r) => {
                println!("Forwarding");
                r.async_send(Data)
            }
            None => (),
        }
    }
}

#[actix::main]
async fn main() {
    let a1 = MyModule::<NullModuleRef>{reply_to: None};
    let h1 = ActixModule::new(a1);
    let h1_addr = h1.start();
    let h1_ref = ActixModuleRef::<_, _, true>::new(h1_addr);

    let a2 = MyModule{reply_to: Some(h1_ref)};
    let h2 = ActixModule::new(a2);
    let h2_addr = h2.start();
    let mut h2_ref = ActixModuleRef::<_, _, true>::new(h2_addr);

    h2_ref.async_send(Data{});

    tokio::time::sleep(Duration::from_secs(1)).await;
}

// use actix::dev::{MessageResponse, OneshotSender};
// use actix::prelude::*;

// #[derive(Message, Debug)]
// #[rtype(result = "Responses")]
// enum Messages {
//     Ping,
//     Pong,
// }

// enum Responses {
//     GotPing,
//     GotPong,
// }

// impl<A, M> MessageResponse<A, M> for Responses
// where
//     A: Actor,
//     M: Message<Result = Responses>,
// {
//     fn handle(self, _ctx: &mut A::Context, tx: Option<OneshotSender<M::Result>>) {
//         if let Some(tx) = tx {
//             let _ = tx.send(self);
//         }
//     }
// }

// // Define actor
// struct MyActor;

// // Provide Actor implementation for our actor
// impl Actor for MyActor {
//     type Context = Context<Self>;

//     fn started(&mut self, _ctx: &mut Context<Self>) {
//         println!("Actor is alive");
//     }

//     fn stopped(&mut self, _ctx: &mut Context<Self>) {
//         println!("Actor is stopped");
//     }
// }

// /// Define handler for `Messages` enum
// impl Handler<Messages> for MyActor {
//     type Result = Responses;

//     fn handle(&mut self, msg: Messages, _ctx: &mut Context<Self>) -> Self::Result {
//         println!("{:?}", msg);
//         match msg {
//             Messages::Ping => Responses::GotPing,
//             Messages::Pong => Responses::GotPong,
//         }
//     }
// }

// #[actix::main]
// async fn main() {
//     // Start MyActor in current thread
//     let addr = MyActor.start();

//     // Send Ping message.
//     // send() message returns Future object, that resolves to message result
//     let ping_future = addr.send(Messages::Ping).await;
//     let pong_future = addr.send(Messages::Pong).await;

//     match pong_future {
//         Ok(res) => match res {
//             Responses::GotPing => println!("Ping received"),
//             Responses::GotPong => println!("Pong received"),
//         },
//         Err(e) => println!("Actor is probably dead: {}", e),
//     }

//     match ping_future {
//         Ok(res) => match res {
//             Responses::GotPing => println!("Ping received"),
//             Responses::GotPong => println!("Pong received"),
//         },
//         Err(e) => println!("Actor is probably dead: {}", e),
//     }
// }