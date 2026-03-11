# bftgrid Copilot Instructions

## Architecture Overview
bftgrid is an actor-based framework for implementing distributed protocols like BFT consensus. It provides a reactive actor model with support for deterministic simulation testing. The framework abstracts over runtime environments (threads, tokio, simulation) through trait-based design.

Key components:
- **bftgrid-core**: Core traits (`ActorMsg`, `ActorRef`, `ActorSystemHandle`, `P2PNetworkClient`) defining the actor API
- **bftgrid-mt**: Multi-threaded implementations (thread-based and tokio-based actor systems)
- **bftgrid-sim**: Single-threaded simulation environment for deterministic testing
- **bftgrid-example**: Usage examples showing actor creation and message passing
- **bftgrid-node**: Node-level abstractions (TBD)

## Core Patterns
- **Messages**: Implement `ActorMsg` trait with `Clone + Debug + Send + 'static`. Use `#[derive(Clone, Debug)]` for simple structs.
- **Actors**: Create structs implementing `TypedMsgHandler<MsgT>` with `receive(&mut self, message: MsgT) -> Option<ActorControl>`.
- **Actor Creation**: Use `ActorSystemHandle::create(node_id, name, join_on_drop)` to spawn actors.
- **Message Sending**: Call `actor_ref.send(message, delay)` for async sends with optional delay.
- **Simulation Compatibility**: Avoid async/await in public APIs; use `spawn_async_send` or `spawn_thread_blocking_send` for async operations.

## Development Workflow
- **Environment**: Use `nix develop` (via direnv) for reproducible Rust toolchain and tools.
- **Build**: `cargo build --workspace` to compile all crates.
- **Test**: `cargo test --workspace` for unit tests; use simulation crate for integration testing.
- **Examples**: Run with `cargo run --bin <example>` from bftgrid-example.
- **Debugging**: Simulation runs single-threaded for deterministic replay; use thread-based impl for concurrent debugging.

## Adding New Features
- **New Message Types**: Define in appropriate crate, implement `ActorMsg`.
- **New Actors**: Create in bftgrid-example or new crate; follow pattern from `local.rs` or `network.rs`.
- **Runtime Support**: Extend `bftgrid-mt` for new async runtimes; implement core traits.
- **Protocols**: Build on top of actor framework; use P2PNetworkClient for cross-node communication.

## Key Files
- `bftgrid-core/src/actor.rs`: Core actor traits and erased types
- `bftgrid-mt/src/thread.rs`: Thread-based actor system implementation
- `bftgrid-sim/src/lib.rs`: Simulation actor system
- `bftgrid-example/src/local.rs`: Example of local actor communication
- `nix/flake.nix`: Development environment definition

## Conventions
- Use erased types (`erased::DynActorRef`) for dynamic dispatch across actor systems.
- Prefer trait objects over generics when crossing crate boundaries.
- Implement `Debug` and `Clone` for all messages and actor states.
- Use `thiserror` for error types; avoid panics in actor handlers.