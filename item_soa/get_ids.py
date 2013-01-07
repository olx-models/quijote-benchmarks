import MySQLdb
import random


DBS = {'db-dev': {'host':   'db-dev.olx.com.ar',
                  'user':   'dev_core',
                  'passwd': '*********',
                  'db':     'DBOLX_DEV'},
       'qa1':    {'host':   'db1-qa1.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_QA'},
       'prod':   {'host':   'replica-olx-main.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_1'}
      }


def get_ids(limit):
    conn = MySQLdb.connect(**DBS['qa1'])
    cur = conn.cursor()
    cur.execute("""SELECT id FROM olx_items WHERE data_domain_id=1
                   ORDER BY id DESC LIMIT 1000,6000""")

    ids = []
    while True:
        row = cur.fetchone()
        if not row:
            break
        ids.append(row[0])

    conn.close()
    
    random.shuffle(ids)
    with open('ids.py', 'w') as f:
        f.write('ids=(%s\n)' % "".join(['\n    %s,' % id for id in ids]))


if __name__ == '__main__':
    get_ids(10000)
