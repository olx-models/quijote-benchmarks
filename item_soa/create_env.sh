#!/bin/bash
echo "Creating virtualenv.."
virtualenv --no-site-packages env

echo -e "\nActivating virtualenv"
source env/bin/activate

echo -e "\nInstalling FunkLoad"
pip install funkload

echo -e "\nDone"
deactivate
