install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

test:
	python -m pytest -v -s --show-capture=all tests/test_cloud.py

format:
	black tests
	black *.py

lint:
	pylint --disable=R,C *.py

all: install test format