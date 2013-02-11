#!/bin/bash
ITEMS=${1:-10000}
source env/bin/activate
python get_ids.py --env=dev --items=$ITEMS
python get_ids.py --env=qa1 --items=$ITEMS
python get_ids.py --env=qa2 --items=$ITEMS
#python get_ids.py --env=live --items=$ITEMS
cp ids_*.py bench
cp ids_*.py funkload
cp ids_*.py freq
rm ids_*.py
deactivate
