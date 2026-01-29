{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";

  outputs = {nixpkgs, ...}: let
    system = "x86_64-linux";
    pkgs = import nixpkgs {
      inherit system;
    };

    shell = pkgs.mkShell {
      name = "dev-shell";
      packages = with pkgs; [
        rustc
        cargo
        clippy
        rustfmt
        pkg-config
        mold
        cargo-watch
      ];
      RUSTFLAGS = "-C link-arg=-fuse-ld=mold";
    };
  in {
    devShells.${system}.default = shell;
  };
}
