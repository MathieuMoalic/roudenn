{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";

  outputs = {nixpkgs, ...}: let
    system = "x86_64-linux";
    pkgs = import nixpkgs {inherit system;};

    grafanaHome = "${pkgs.grafana}/share/grafana";

    runGrafana = pkgs.writeShellApplication {
      name = "run-grafana";
      runtimeInputs = with pkgs; [grafana coreutils];
      text = ''
        set -euo pipefail

        ROOT="$(pwd)"
        STATE_DIR="''${ROOT}/.grafana"

        mkdir -p \
          "''${STATE_DIR}/data" \
          "''${STATE_DIR}/logs" \
          "''${STATE_DIR}/plugins" \
          "''${STATE_DIR}/provisioning/datasources" \
          "''${STATE_DIR}/provisioning/dashboards"

        # Local-dev defaults (override via env vars)
        : "''${GRAFANA_ADDR:=127.0.0.1}"
        : "''${GRAFANA_PORT:=3000}"
        : "''${GRAFANA_ADMIN_USER:=admin}"
        : "''${GRAFANA_ADMIN_PASSWORD:=admin}"

        export GF_PATHS_DATA="''${STATE_DIR}/data"
        export GF_PATHS_LOGS="''${STATE_DIR}/logs"
        export GF_PATHS_PLUGINS="''${STATE_DIR}/plugins"
        export GF_PATHS_PROVISIONING="''${STATE_DIR}/provisioning"

        export GF_SERVER_HTTP_ADDR="''${GRAFANA_ADDR}"
        export GF_SERVER_HTTP_PORT="''${GRAFANA_PORT}"

        export GF_SECURITY_ADMIN_USER="''${GRAFANA_ADMIN_USER}"
        export GF_SECURITY_ADMIN_PASSWORD="''${GRAFANA_ADMIN_PASSWORD}"

        echo "Grafana state: ''${STATE_DIR}"
        echo "Grafana URL:   http://''${GRAFANA_ADDR}:''${GRAFANA_PORT}"
        echo "Login:        ''${GRAFANA_ADMIN_USER} / ''${GRAFANA_ADMIN_PASSWORD}"

        exec grafana-server \
          --homepath ${grafanaHome}
      '';
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
        sqlite

        # optional but convenient:
        grafana
        runGrafana
      ];
      RUSTFLAGS = "-C link-arg=-fuse-ld=mold";
    };
  in {
    devShells.${system}.default = shell;

    packages.${system}.run-grafana = runGrafana;

    apps.${system}.grafana = {
      type = "app";
      program = "${runGrafana}/bin/run-grafana";
    };
  };
}
