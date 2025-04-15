# Actor examples

This folder contains a local and a networked actor system, each with a simulation test.

The message handling and overall system construction logic is shared between the actual programs,
that run on multiple multi-threaded actor systems, showcasing interactions between actors managed
by different actor systems, and the tests, that run on the single-threaded simulation testing actor
system.

The examples can be run as follows:

```bash
> cargo run --bin bftgrid-local-example
> cargo run --bin bftgrid-network-example
```

Simulation tests are regular tests and can be run as usual e.g. with `cargo test`.
