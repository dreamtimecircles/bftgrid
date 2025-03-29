# `bftgrid`

An actor-like reactive style framework with support for simulation testing
to implement and test distributed protocols like BFT consensus protocols.

Plese refer to the docs and READMEs in the subdirs for more information and example usage.

## Docs

The `rustdoc` reference including an index can be found at the [GitHub Pages site];
look for the relevant modules, at the moment [`bftgrid-core`].

[GitHub Pages site]: https://dreamtimecircles.github.io/bftgrid
[`bftgrid-core`]: https://dreamtimecircles.github.io/bftbench/bftgrid_core

## Development

This repository uses [Nix] and [direnv] to provide a reproducible development environment.

[Nix]: https://nixos.org
[direnv]: https://direnv.net

## TODOs

- [ ] Actual protocol impl. w/benchmarking via [`bftbench`]
- [ ] Make `P2PNetworkError` independent from the runtime framework

[`bftbench`]: https://github.com/dreamtimecircles/bftbench
