.PHONY: install dev test cov lint fmt migrate docker-up docker-down loadtest

install:
	pip install -r requirements-dev.txt

dev:
	uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

test:
	pytest

cov:
	pytest --cov --cov-report=term-missing

lint:
	ruff check .

fmt:
	ruff check --fix .

migrate:
	alembic upgrade head

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

loadtest:
	locust -f loadtest/locustfile.py --host http://localhost:8000
