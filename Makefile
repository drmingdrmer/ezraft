all: check_all test

check_all: lint doc unused_dep typos

test:
	cargo test

# Format, then lint exactly as CI does, so `make lint` fails where CI would.
lint:
	cargo fmt
	cargo clippy --no-deps --all-targets -- -D warnings
	@# Bug: clippy --all-targets reports false warning about unused dep in
	@# `[dev-dependencies]`:
	@# https://github.com/rust-lang/rust/issues/72686#issuecomment-635539688
	@# Thus we only check unused deps for lib
	RUSTFLAGS=-Wunused-crate-dependencies cargo clippy --no-deps --lib -- -D warnings

fmt:
	cargo fmt

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --all --no-deps

check:
	RUSTFLAGS="-D warnings" cargo check --all-targets

unused_dep:
	# cargo install cargo-machete
	cargo machete

typos:
	# cargo install typos-cli
	typos --write-changes src/ examples/ tests/ docs/ README.md

audit:
	# cargo install cargo-audit
	cargo audit

# Verify the crate packages and publishes cleanly, without uploading.
publish_check:
	cargo publish --dry-run

clean:
	cargo clean
	@# Cluster state the kvstore example writes, relative to where it ran. A
	@# stale directory makes the next run come back with the old node id and
	@# log instead of forming a fresh cluster. Mirrors the .gitignore entry.
	rm -rf data

.PHONY: all check_all test lint fmt doc check unused_dep typos audit publish_check clean
