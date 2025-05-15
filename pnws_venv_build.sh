#!/bin/bash
ENVNAME=pnws_env

python3 -m venv $ENVNAME
source $ENVNAME/bin/activate
python3 -m pip install --upgrade pip
pip install -r requirements.txt
