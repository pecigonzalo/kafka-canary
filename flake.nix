{
  description = "A Nix wrapped development environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-22.11-small";
    nixpkgs-unstable.url = "github:nixos/nixpkgs/nixos-unstable-small";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, nixpkgs-unstable, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        pkgs-unstable = nixpkgs-unstable.legacyPackages.${system};
      in
      {
        devShell = pkgs.mkShell
          rec {
            buildInputs = with pkgs;
              [
                go_1_20
                gopls
                goreleaser
                gnumake
                reftools
                ginkgo
                gotools
                gomodifytags
                iferr
                impl
                go-mockery
              ];
          };
      });
}
