SHELL := /bin/bash

setup: env-setup module-setup

remove: env-remove

env-setup:
	conda env create -f environment.yml
	conda activate pambdas
	source activate pambdas

module-setup:
	pip install -e ./

env-remove:
	conda env remove -n pambdas -y

test:
	pytest ./tests/ -x

black:
	black --check ./