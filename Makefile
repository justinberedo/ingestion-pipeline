.PHONY: dev tables connect_db insert_data

check_python_version:
	@python_version=$$(python3 --version 2>&1 | cut -d ' ' -f 2); \
	if [ "$$python_version" \< "3.10" ]; then \
		echo "Error: This project requires Python 3.10 or higher. Your system is using Python $$python_version"; \
		exit 1; \
	fi

dev: check_python_version
	python3 -m venv .venv
	. .venv/bin/activate
	pip install -r requirements.txt

HOST=localhost
DB_USERNAME=db_user
PORT=5432
DATABASE_NAME=first_circle_db

tables:
	psql -U ${DB_USERNAME} \
		-h ${HOST} \
		-p ${PORT} \
		-d ${DATABASE_NAME} \
		-f tables.sql

connect_db:
	psql -U ${DB_USERNAME} \
		-h ${HOST} \
		-p ${PORT} \
		-d ${DATABASE_NAME}
