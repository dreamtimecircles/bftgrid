{ pkgs ? import (builtins.fetchTarball {
           # Descriptive name to make the store path easier to identify
           name = "nixos-unstable-2024-01-15";
           # Commit hash for nixos-unstable as of 2023-05-18
           url = "https://github.com/nixos/nixpkgs/archive/a382cd09c265d1951c3ea6c7a1de61d8318b1353.tar.gz";
           # Hash obtained using `nix-prefetch-url --unpack <url>`
           sha256 = "1dza0p0fyclk3nw84i8y1mb0g4r1vq2330h5ygnsyrv95p9p94li";
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
