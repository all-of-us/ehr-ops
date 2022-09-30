#!/usr/bin/env bash
#
# Setup virtual environment and install python library requirements.
#
python3 -m venv venv
source venv/bin/activate
echo "Installing python modules..."
pip install -q --upgrade pip

export PYTHONPATH=`pwd`
echo "PYTHONPATH=${PYTHONPATH}"
pip install -q -r requirements.txt
pip list