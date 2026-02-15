.PHONY: all
all: lint test build

.PHONY: build
build:
	cargo build --release

.PHONY: test
test:
	cargo test -- --test-threads=1

.PHONY: lint
lint:
	cargo clippy -- -D warnings
