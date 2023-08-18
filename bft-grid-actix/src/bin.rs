use actix::prelude::*;
use bft_grid_core::*;
use bft_grid_actix::*;

struct MyModule {}

struct Data;

impl MessageHandler<Data> for MyModule {
    fn receive(&mut self, _message: Data) -> () {
        ()
    }
}

#[actix::main] // <- starts the system and block until future resolves
async fn main() {
    let a = MyModule{};
    let h = AsyncActixModule::<Data, MyModule>::new(a);
    let actix_addr = h.start();
    let mut addr = AsyncActixModuleRef::new(actix_addr);
    let res = addr.async_send(Data{});
    print!("{:?}", res);
}
