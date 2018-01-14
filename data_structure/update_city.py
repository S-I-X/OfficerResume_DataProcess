# -*- coding:utf-8 _-*-
# !/usr/bin/python


from data_structure.csv_to_postgre import getCon
from data_structure.csv_to_postgre import super_cities

if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, province, city, place_of_birth FROM crawler.officer_message WHERE province='';"
    update_sql = "UPDATE crawler.officer_message SET province = '{0}', city = '{1}' WHERE id_index = '{2}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, province, city, place_of_birth = line
        for super_city in super_cities:
            if super_city in place_of_birth:
                province = super_city
                city = super_city
                finish_update_sql = update_sql.format(province, city, id_index)
                cur.execute(finish_update_sql)
                Connection.commit()
                print(finish_update_sql)
                print(id_index, '的城市信息已更新')
    Connection.close()

