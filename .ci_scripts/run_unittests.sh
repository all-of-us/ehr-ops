#!/usr/bin/env bash
#
# Run python unittests
#
source venv/bin/activate
export PYTHONPATH=`pwd`

python -m unittest discover -s tests