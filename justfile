default: hot-reload

hot-reload:
        cargo watch -q -c -w src -w Cargo.toml -x 'run -- '
