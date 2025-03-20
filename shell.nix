{ pkgs ? import (builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixos-unstable-2025-03-17_9_58_GMT+1";
  url = "https://github.com/nixos/nixpkgs/archive/21422b97b86323f2d6fff2dfe5bab3a0e465a99a.tar.gz";
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "0s0j16h57ckrmh9c20m1db5c0nb6ij92gdr3da0jh9p6jwklq4y1";
}) {} }:

with pkgs;

mkShell {
  buildInputs = [
    protobuf
    git
    python310Packages.mdformat
    cargo
    clippy
    rustc
    zlib
    zip
  ];
}
