pub trait ActorHandler<M> {
    fn receive(&mut self, message: M) -> ();
}

pub trait ModuleRef<M> {
    fn async_send(message: M) -> ();
}
