CARGO_FLAGS ?=

.PHONY: all fmt fmt-check check check-all-features check-no-default lint lint-fix test test-all test-no-default doc security ci build publish-dry release release-snapshot clean

ci: lint check check-no-default test test-all test-no-default doc security

all: ci

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

check:
	cargo check --workspace --all-targets $(CARGO_FLAGS)

check-all-features:
	cargo check --workspace --all-targets --all-features $(CARGO_FLAGS)

check-no-default:
	cargo check --workspace --all-targets --no-default-features $(CARGO_FLAGS)

lint-fix: fmt
	cargo clippy --workspace --all-targets --all-features --fix --allow-dirty --allow-staged

lint: fmt-check
	cargo clippy --workspace --all-targets --all-features $(CARGO_FLAGS) -- -D warnings

test test-all test-no-default:
	cargo test --workspace $(patsubst test-%,--%-features,$(filter-out test,$@)) $(CARGO_FLAGS)

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features $(CARGO_FLAGS)

security:
	cargo deny check

build:
	cargo build --workspace $(CARGO_FLAGS)

publish-dry:
	cargo publish --dry-run --allow-dirty

release: RELEASE_VERSION = $(VERSION)
release-snapshot: RELEASE_VERSION = $(VERSION)-next

release release-snapshot:
	@test -n "$(VERSION)" || (echo "VERSION is required" >&2; exit 1)
	mkdir -p dist
	git archive --format=tar.gz --prefix="$(notdir $(CURDIR))-$(RELEASE_VERSION)/" -o "dist/$(notdir $(CURDIR))_$(RELEASE_VERSION)_source.tar.gz" HEAD Cargo.toml Cargo.lock LICENSE README.md src

clean:
	cargo clean
	rm -rf dist
