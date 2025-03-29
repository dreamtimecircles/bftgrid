# Thread- and Tokyo-based (async) actor systems

Two production actor systems implementations are provided: one that uses native OS threads for actors,
suitable for compute-heavy actor handlers, and one based on the Tokyo async runtime, suitable for
I/O heavy actor handlers.

A UDP-based Tokio/async P2P network implementation is also included.

Please refer to the docs for information about the framework and to [`bftgrid-example`](../bftgrid-example)
for usage examples.
