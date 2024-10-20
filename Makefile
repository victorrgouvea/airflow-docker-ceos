kill: docker-compose down --volumes --rmi all

up-airflow: docker-compose up airflow-init -d

up: docker-compose up -d

down: docker-compose down -d

build: docker-compose build