#!/usr/bin/env python

import argparse
import MySQLdb
import random


DBS = {'dev':    {'host':   'db-dev.olx.com.ar',
                  'user':   'dev_core',
                  'passwd': '+qyvpis6l#',
                  'db':     'DBOLX_DEV'},
       'qa1':    {'host':   'db1-qa1.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_QA'},
       'qa2':    {'host':   'db2-qa2.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_QA'},
       'live':   {'host':   'replica-olx-main.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_1'}
      }


def get_ids(env, items):
    conn = MySQLdb.connect(**DBS[env])
    cur = conn.cursor()
    cur.execute("""SELECT id FROM olx_items WHERE data_domain_id=1
                   ORDER BY id DESC LIMIT 5000,%s""" % items)

    ids = []
    while True:
        row = cur.fetchone()
        if not row:
            break
        ids.append(row[0])

    conn.close()

    random.shuffle(ids)
    filename = 'ids_%s.py' % env
    with open(filename, 'w') as f:
        f.write('ids=[%s\n]' % "".join(['\n    %s,' % id for id in ids]))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get item ids')
    parser.add_argument('--env', type=str, required=True,
                        help='dev, qa1, qa2, live')
    parser.add_argument('--items', type=int, required=True,
                        help='Amount of ids')
    args = parser.parse_args()
    get_ids(args.env, args.items)
