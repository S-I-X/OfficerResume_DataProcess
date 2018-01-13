# -*- coding:utf-8 _-*-
# !/usr/bin/python
import time

import re

from data_structure.csv_to_postgre import getCon

if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, date_of_birth FROM crawler.officer_message;"
    update_sql = "UPDATE crawler.officer_message SET age = '{0}' WHERE id_index = '{1}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        officer_id, date_of_birth = line
        date = re.findall('\d\d\d\d', date_of_birth)
        if len(date) != 0:
            age = time.gmtime().tm_year - int(date[0])
            # print(age)
            finish_update_sql = update_sql.format(str(age), officer_id)
            cur.execute(finish_update_sql)
            Connection.commit()
    Connection.close()

