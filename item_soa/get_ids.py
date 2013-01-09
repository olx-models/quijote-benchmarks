import sys
import MySQLdb
import random


DBS = {'dev':    {'host':   'db-dev.olx.com.ar',
                  'user':   'dev_core',
                  'passwd': '*********',
                  'db':     'DBOLX_DEV'},
       'qa1':    {'host':   'db1-qa1.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_QA'},
       'qa2':    {'host':   'db2-qa2.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_QA'},
       'prod':   {'host':   'replica-olx-main.olx.com.ar',
                  'user':   'dev',
                  'passwd': '*********',
                  'db':     'DBOLX_1'}
      }


def get_ids(filename, host, limit):
    conn = MySQLdb.connect(**DBS[host])
    cur = conn.cursor()
    cur.execute("""SELECT id FROM olx_items WHERE data_domain_id=1
                   ORDER BY id DESC LIMIT 5000,%s""" % limit)

    ids = []
    while True:
        row = cur.fetchone()
        if not row:
            break
        ids.append(row[0])

    conn.close()
    
    random.shuffle(ids)
    with open(filename, 'w') as f:
        f.write('ids=(%s\n)' % "".join(['\n    %s,' % id for id in ids]))


if __name__ == '__main__':
    filename = sys.argv[1]
    host = sys.argv[2]
    limit = sys.argv[3]
    get_ids(filename, host, limit)
