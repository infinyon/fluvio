.PHONY: venv venv-pip

PYTHON=./venv/bin/python
PIP=./venv/bin/pip

venv:
	python -m venv venv

venv-pip: venv
	$(PIP) install -r requirements.txt

build-wheel: venv-pip
	$(PYTHON) setup.py bdist_wheel

build-dev: venv-pip
	$(PYTHON) setup.py develop

test: build-dev
	$(PYTHON) setup.py test

clean:
	rm -r venv fluvio/fluvio_rust.*.so
