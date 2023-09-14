pub trait MessageHandler<M> {
    fn receive(&mut self, message: M) -> ();
}

pub trait ModuleRef<M> {
    fn async_send(&mut self, message: M) -> ();
}

pub trait ModuleSystem {
    fn spawn<M: 'static + Send, MH: 'static + MessageHandler<M> + Send>(handler: MH) -> Box<dyn ModuleRef<M>>;
}
