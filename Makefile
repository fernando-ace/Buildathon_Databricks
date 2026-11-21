IMAGE ?= shopping-sessions:latest
PLATFORM ?= linux/amd64
CPUS ?= 8
MEM ?= 24g

.PHONY: build native check fixtures container-fixtures dev-check reference-check benchmark

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

check:
	cargo fmt --check
	cargo check --release --locked

native:
	cargo build --release --locked

fixtures: native
	python tests/run_fixtures.py

container-fixtures: build
	python tests/run_fixtures.py --docker-image $(IMAGE)

dev-check: native
	python tests/check_dev.py --data dev

reference-check: native
	python tests/check_reference.py --data $(DATA)

benchmark: native
	python tests/benchmark.py --data $(DATA)

