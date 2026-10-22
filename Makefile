.PHONY: up down status test

up:
	docker compose up -d

down:
	docker compose down

status:
	docker compose ps

test:
	docker compose exec -T postgres sh -c 'pg_isready -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'
	docker compose exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -Atqc "SHOW wal_level"' | grep -qx logical
