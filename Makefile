.DEFAULT_GOAL := ci

CARGO_FLAGS ?=

.PHONY: all fmt fmt-check check check-all-features check-no-default lint lint-fix test test-all test-no-default doc security ci build publish-dry release release-snapshot clean

all: ci

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

check:
	cargo check --workspace --all-targets $(CARGO_FLAGS)

check-all-features:
	cargo check --workspace --all-targets --all-features

check-no-default:
	cargo check --workspace --all-targets --no-default-features

lint-fix: fmt
	cargo clippy --workspace --all-targets --all-features --fix --allow-dirty --allow-staged

lint: fmt-check
	cargo clippy --workspace --all-targets --all-features $(CARGO_FLAGS) -- -D warnings

test:
	cargo test --workspace $(CARGO_FLAGS)

test-all:
	cargo test --workspace --all-features $(CARGO_FLAGS)

test-no-default:
	cargo test --workspace --no-default-features $(CARGO_FLAGS)

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features $(CARGO_FLAGS)

security:
	cargo deny check

ci: lint check check-all-features check-no-default test test-all test-no-default doc security

build:
	cargo build --workspace $(CARGO_FLAGS)

publish-dry:
	cargo publish --dry-run --allow-dirty

release:
	node scripts/release-metadata.js
	goreleaser release --clean --skip=publish,validate --config target/goreleaser.json

release-snapshot:
	node scripts/release-metadata.js
	goreleaser release --snapshot --clean --skip=publish,validate --config target/goreleaser.json

clean:
	cargo clean
	rm -rf dist
