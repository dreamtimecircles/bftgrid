{ pkgs ? import (builtins.fetchTarball {
           # Descriptive name to make the store path easier to identify
           name = "nixos-unstable-2023-08-17";
           # Commit hash for nixos-unstable as of 2023-05-18
           url = "https://github.com/nixos/nixpkgs/archive/36e94693b7f938223a5bd149f2fb9f6417ddb676.tar.gz";
           # Hash obtained using `nix-prefetch-url --unpack <url>`
           sha256 = "0jv9305h4h9wr4k2spyg7x3nnw22k2y8920n978pxdywhnyx4znj";
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
