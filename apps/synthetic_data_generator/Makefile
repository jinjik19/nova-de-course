CONTAINER_PROJECT_DIR := /opt/spark/apps/synthetic_data_generator
CUR_DIR := $(shell pwd)

.PHONY: lint format run clean
count ?= 1000

lint:
	ruff check .

format:
	ruff check --fix .
	ruff format .

run:
	PYSPARK_PYTHON=$(CUR_DIR)/.venv/bin/python \
	PYSPARK_DRIVER_PYTHON=$(CUR_DIR)/.venv/bin/python \
	python main.py --count $(count)

clean:
	rm -rf output/
	rm -f src.zip
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +