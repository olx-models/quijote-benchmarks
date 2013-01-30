#!/bin/bash
echo "Creating virtualenv.."
virtualenv --no-site-packages env

echo -e "\nActivating virtualenv"
source env/bin/activate

echo -e "\nInstalling modules"
pip install requests

echo -e "\nDone"
deactivate
