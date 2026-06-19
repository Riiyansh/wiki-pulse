up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

producer-logs:
	docker compose logs -f producer

spark-logs:
	docker compose logs -f spark

restart:
	docker compose restart

clean:
	docker compose down -v
