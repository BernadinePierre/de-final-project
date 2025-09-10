###################################################
#                                                 #
#  Makefile for building and running the project  #
#                                                 #
###################################################

# Define variables
REGION = eu-west-2
PYTHON_INTERPRETER ?= python3
PIP ?= pip3
WD = $(shell pwd)
PYTHONPATH = $(WD)
SHELL = /bin/sh
PROFILE = default

# Virtual environment directory
VENV_DIR = venv
VENV_ACTIVATE = $(VENV_DIR)/bin/activate

# Create virtual environment
create-environment:
	@echo ">>> Setting up virtual environment"
	$(PYTHON_INTERPRETER) -m venv $(VENV_DIR)

# Define environment activation
define execute_in_env
	. $(VENV_ACTIVATE) && $1
endef

# Install requirements
build-requirements:
	@echo ">>> Installing requirements"
	$(call execute_in_env, $(PIP) install -r requirements.txt)

# Combined target to set up environment and install requirements
make-build: create-environment build-requirements

# Set up directories for Lambda
create-folders:
	@echo ">>> Creating folders for Lambda files"
	@mkdir -p terraform/lambdas
	@mkdir -p terraform/libraries/source/python


# Initialize Terraform
init-terraform:
	@echo ">>> Initializing Terraform"
	@cd terraform && \
	terraform init

# Combined target to set up Terraform
setup-terraform: create-folders create-library init-terraform