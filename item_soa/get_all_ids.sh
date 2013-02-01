#!/bin/bash
source env/bin/activate
python get_ids.py --env=dev --items=10000
python get_ids.py --env=qa1 --items=10000
python get_ids.py --env=qa2 --items=10000
#python get_ids.py --env=live --items=10000
cp ids_*.py bench
cp ids_*.py funkload
rm ids_*.py
deactivate
