###################################################
#                                                 #
#  Makefile for building and running the project  #
#                                                 #
###################################################

REGION = eu-west-2
PYTHON_INTERPRETER = PYTHON_INTERPRETER
WD = $(shell pwd)
PYTHONPATH = $(pwd)
SHELL := /bin/bash
PROFILE = default
PIP := pip

# Create venv environment and fetch required modules

create-environment:
	@echo ">>> Setting up venv"
	( \
		$(PYTHON_INTERPRETER) -m venv venv; \
	)

ACTIVATE_ENV := source venv/bin/activate

define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

build-requirements:
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

make-build:
	create-environment build-requirements

# Set up files for lambda

create-folders:
	@echo ">>> Creating folders for lambda files"
	@-mkdir terraform/lambdas
	@-mkdir terraform/libraries
	@-mkdir terraform/libraries/source
	@-mkdir terraform/libraries/source/python

create-library:
	@echo ">>> Creating lambda layer"
	@$(call execute_in_env, $(PIP) install --target ./terraform/libraries/source/python -r requirements.txt)

init-terraform:
	@echo ">>> Initialising Terraform"
	@cd ./terraform; \
	echo ">> Working in $(pwd)"; \
	terraform init; \

setup-terraform: create-folders create-library init-terraform