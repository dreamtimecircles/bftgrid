{ pkgs ? import (builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixos-unstable-2024-01-15";
  # Commit hash for nixos-unstable as of 2023-05-18
  url = "https://github.com/nixos/nixpkgs/archive/57746ceea592f88f1b7f847a9b724c2429540ef7.tar.gz";
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "0k2j7lsyjwnxb5kfarn3qdq5ivi8rhrrr15vm3warjyaavlyj4sl";
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
