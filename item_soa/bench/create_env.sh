#!/bin/bash

MODULES="requests==1.1.0"

echo "Creating virtualenv.."
virtualenv --python=python2.6 env

echo -e "\nActivating virtualenv.."
source env/bin/activate

echo -e "\nInstalling modules $MODULES.."
pip install $MODULES

if [ $? -eq "0" ]
then
    echo -e "\nVirtualenv done!"
    deactivate
else
    echo -e "\nError installing modules!"
    rm -r env
fi
