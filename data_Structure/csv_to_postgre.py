# -*- coding:utf-8 _-*-
# !/usr/bin/python
import csv
import time

import psycopg2


def getCon(database=None, user=None, password=None, host=None, port='5432'):
    conn = psycopg2.connect(database, user, password, host, port)
    print("Connection successful.")
    return conn


def insertMessage(Conn=None, data=None):
    cur = Conn.cursor()
    cur.execut("INSERT INTO crawler.officer_message VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6})".format(data[0],
                                                                                                       data[1], data[2],
                                                                                                       data[3], data[4],
                                                                                                       data[5], data[6]))
    cur.commit()


def getData(file='官员信息-1-8.csv'):
    with open(file, 'r') as csv_file:
        lines = csv.reader(csv_file, delimiter=' ', quotechar='|')
        return lines


def makeupData(line):
    fabric_data = dict()
    half_message = fabric_data[0][0].split(sep=',')
    fabric_data['id_index'] = half_message[0]
    fabric_data['office_name'] = half_message[1]
    fabric_data['baike_url'] = half_message[2]
    fabric_data['head_image_url'] = half_message[3]
    split_half_message = half_message[4].split(sep='，')
    fabric_data['gender'] = split_half_message[1]
    fabric_data['nation'] = split_half_message[2]
    fabric_data['province'] = split_half_message[4][:2]
    fabric_data['city'] = split_half_message[4][2:-1]
    fabric_data['date_of_birth'] = split_half_message[3]
    fabric_data['place_of_birth'] = split_half_message[4]
    fabric_data['educational_background'] =
    return fabric_data


if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    for line in getData():
        data = makeupData(line)
        insertMessage(Connection, data)
    Connection.commit()
    Connection.close()
