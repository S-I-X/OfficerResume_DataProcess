# -*- coding:utf-8 _-*-
# !/usr/bin/python
import csv
import time

import psycopg2
import re

provinces = ('河北', '山西', '内蒙', '辽宁', '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建', '江西', '山东', '河南',
             '湖北', '湖南', '广东', '广西', '海南', '四川', '贵州', '云南', '陕西', '甘肃', '青海', '宁夏', '新疆')


def getCon(database=None, user=None, password=None, host=None, port='5432'):
    conn = psycopg2.connect(database, user, password, host, port)
    print("Connection successful.")
    return conn


def insertMessage(Conn=None, one_data=None):
    cur = Conn.cursor()
    cur.execut("""INSERT INTO crawler.officer_message VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, 
    {9}, {10}, {11}, {12}, {13}, {14})""".format(one_data['id_index'], one_data['officer_name'], one_data['baike_url'],
                                                 one_data['head_image_url'], one_data['gender'], one_data['nation'],
                                                 one_data['province'], one_data['city'], one_data['date_of_birth'],
                                                 one_data['place_of_birth'], one_data['time_and_job'],
                                                 one_data['educational_background'],
                                                 one_data['educational_university'], one_data['current_site'],
                                                 one_data['message_source'],
                                                 one_data['relative_person_links']))
    cur.commit()


def getData(file='../data/官员信息-1-8.csv'):
    with open(file, 'r') as csv_file:
        lines = csv.reader(csv_file)
        return lines


def makeupData(one_line):
    fabric_data = dict()
    fabric_data['id_index'] = one_line[0]
    fabric_data['office_name'] = one_line[1]
    fabric_data['baike_url'] = one_line[2]
    fabric_data['head_image_url'] = one_line[3]
    half_message = one_line[4]
    fabric_data['gender'] = '男' if '男' in half_message else '女'
    fabric_data['nation'] = half_message[half_message.index('族') - 1: half_message.index('族') + 1]
    split_half_message = half_message.split(sep='，')
    fabric_data['place_of_birth'] = ''
    fabric_data['province'] = ''
    fabric_data['city'] = ''
    for mess in split_half_message:
        if mess[-1] == '人' or mess[:2] == '籍贯':
            fabric_data['place_of_birth'] = mess[2:] if '籍贯' in mess else mess[:-1]
            for province in provinces:
                if province in mess:
                    fabric_data['province'] = province
                    fabric_data['city'] = mess[2 + len(province):] if '籍贯' in mess else mess[len(province):-1]

    try:
        mark = half_message.index("月出生")
    except ValueError:
        try:
            mark = half_message.index("月生")
        except ValueError:
            mark = 0

    fabric_data['date_of_birth'] = half_message[half_message.index("年"): mark + 1] if mark != 0 else ''
    educational_background = set()
    educational_background.add('大专') if '大专' in half_message else None
    educational_background.add('大学学历') if '大学学历' in half_message else None
    educational_background.add('本科') if '本科' in half_message else None
    educational_background.add('学士') if '学士' in half_message else None
    educational_background.add('研究生') if '研究生' in half_message else None
    educational_background.add('硕士') if '硕士' in half_message else None
    educational_background.add('博士') if '博士' in half_message else None
    fabric_data['educational_background'] = str(educational_background)
    mid_messages = re.sub("\'", '', one_line[5][1:-1]).split(sep=',')
    mid_messages_dict = dict()
    for mid_message in mid_messages:
        key, value = mid_message.split(sep=':')
        mid_messages_dict[key.replace(' ', '')] = value.replace(' ', '')
    try:
        fabric_data['educational_university'] = mid_messages_dict['毕业院校']
    except KeyError:
        fabric_data['educational_university'] = ''

    return fabric_data


if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    for line in getData():
        data = makeupData(line)
        insertMessage(Connection, data)
    Connection.commit()
    Connection.close()
