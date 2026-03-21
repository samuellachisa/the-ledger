.PHONY: help install dev-up dev-down test test-ci test-property generate-key migrate lint typecheck k8s-apply

help:
	@echo "Available targets:"
	@echo "  install          Install dependencies"
	@echo "  dev-up           Start postgres (docker-compose)"
	@echo "  dev-down         Stop postgres"
	@echo "  test             Run test suite (requires postgres_test running)"
	@echo "  test-ci          Spin up test DB, run tests, tear down"
	@echo "  test-property    Run property-based tests only"
	@echo "  generate-key     Generate a new FIELD_ENCRYPTION_KEY"
	@echo "  migrate          Apply schema to DATABASE_URL"
	@echo "  lint             Run ruff linter"
	@echo "  typecheck        Run mypy"
	@echo "  k8s-apply        Apply all Kubernetes manifests"

install:
	pip install -r requirements.txt

dev-up:
	docker-compose up -d postgres
	@echo "Waiting for postgres..."
	@until docker-compose exec postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 1; done
	@echo "Postgres ready."

dev-down:
	docker-compose down

test:
	pytest tests/ -v

test-ci:
	docker-compose up -d postgres_test
	@until docker-compose exec postgres_test pg_isready -U postgres > /dev/null 2>&1; do sleep 1; done
	TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5433/apex_financial_test pytest tests/ -v
	docker-compose stop postgres_test

test-property:
	pytest tests/test_property_based.py -v

generate-key:
	@python -c "import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"

migrate:
	@python scripts/migrate.py

lint:
	ruff check src/ tests/

typecheck:
	mypy src/ --ignore-missing-imports

k8s-apply:
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/secret.yaml
	kubectl apply -f k8s/deployment.yaml
	kubectl apply -f k8s/service.yaml
	kubectl apply -f k8s/hpa.yaml
	kubectl apply -f k8s/pdb.yaml
