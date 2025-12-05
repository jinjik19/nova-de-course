.PHONY: lint format run clean
count ?= 1000

lint:
	ruff check .

format:
	ruff check --fix .
	ruff format .

run:
	PYSPARK_PYTHON=$(shell pwd)/.venv/bin/python \
	PYSPARK_DRIVER_PYTHON=$(shell pwd)/.venv/bin/python \
	python main.py --count $(count)

clean:
	rm -rf output/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +