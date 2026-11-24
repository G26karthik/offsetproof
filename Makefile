.PHONY: up down seed verify fault diagnose benchmark demo test-unit test-integration lint typecheck

up:
	docker compose up -d
	python -m offsetproof.ops.readiness

down:
	docker compose down

seed:
	python -m offsetproof.producer --seed $(SEED) --events $(EVENTS)

verify:
	python -m offsetproof.ops.verify --manifest results/latest/manifest.json

fault:
	python -m offsetproof.faults.run --scenario $(SCENARIO)

diagnose:
	python -m offsetproof.diagnose --mode $(MODE) --run results/latest

benchmark:
	python -m offsetproof.eval.benchmark --config eval/plans/baseline.yaml

demo:
	python -m offsetproof.ops.demo

test-unit:
	pytest -m unit -q

test-integration:
	pytest -m integration -q

lint:
	ruff check src tests

typecheck:
	mypy src
