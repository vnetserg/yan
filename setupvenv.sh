#!/usr/bin/env bash

VIRTUALENV_PATH=$1

# Check that virtualenv path is set
if [[ ! $VIRTUALENV_PATH ]]; then
    echo "$0: CRITICAL: no virtualenv path supplied"
    exit 1
fi

# If activation script of the virtual environment exists,
# let's hope that it's all good
if [[ -e "$VIRTUALENV_PATH/bin/activate" ]]; then
    exit 0
fi

# Check that virtualenv is present
which virtualenv &> /dev/null
if [[ "$?" -ne 0 ]]; then
  echo "$0: CRITICAL: dependency 'virtualenv' is not installed."
  exit 2
fi

# Create the virtual environment
virtualenv $VIRTUALENV_PATH -p python3
if [[ $? -ne 0 ]]; then
    echo "${0}: CRITICAL: could not create virtual environment under ${VIRTUALENV_PATH}"
    exit 3
fi

# Acivate the environment
source $VIRTUALENV_PATH/bin/activate
if [[ $? -ne 0 ]]; then
    echo "${0}: CRITICAL: could not activate virtual environment: ${VIRTUALENV_PATH}/bin/activate"
    exit 4
fi

# Install Python package
python setup.py install
if [[ $? -ne 0 ]]; then
    echo "${0}: CRITICAL: could not install 'yan' package under Python virtual environment"
    exit 5
fi

exit 0
