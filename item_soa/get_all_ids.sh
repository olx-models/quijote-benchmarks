#!/bin/bash
source env/bin/activate
python get_ids.py --env=dev --items=10000
python get_ids.py --env=qa1 --items=10000
python get_ids.py --env=qa2 --items=10000
python get_ids.py --env=live --items=10000
deactivate
