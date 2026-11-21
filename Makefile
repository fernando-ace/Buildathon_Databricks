IMAGE ?= shopping-sessions:latest
PLATFORM ?= linux/amd64
CPUS ?= 8
MEM ?= 24g

.PHONY: build check fixtures dev-check

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

check:
	cargo fmt --check
	cargo check --release --locked

fixtures: check
	python tests/run_fixtures.py

dev-check: check
	python tests/check_dev.py --data dev

