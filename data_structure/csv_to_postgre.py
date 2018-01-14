# -*- coding:utf-8 _-*-
# !/usr/bin/python
import csv

import psycopg2
import re
from data_structure.process_introduce import process_introduce

provinces = ('河北', '山西', '内蒙', '辽宁', '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建', '江西', '山东', '河南',
             '湖北', '湖南', '广东', '广西', '海南', '四川', '贵州', '云南', '陕西', '甘肃', '青海', '宁夏', '新疆')
super_cities = ('北京', '重庆', '天津', '上海')


def getCon(database=None, user=None, password=None, host=None, port=5432):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    print("Connection successful.")
    return conn


def insertMessage(Conn=None, one_data=None):
    cur = Conn.cursor()
    insert_sql = """INSERT INTO crawler.officer_message VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', 
    '{9}', '{10}', '{11}', '{12}', '{13}', '{14}')""".format(one_data['id_index'], one_data['officer_name'],
                                                             one_data['baike_url'],
                                                             one_data['head_image_url'], one_data['gender'],
                                                             one_data['nation'],
                                                             one_data['province'], one_data['city'],
                                                             one_data['date_of_birth'],
                                                             one_data['place_of_birth'], one_data['time_and_job'],
                                                             one_data['educational_background'],
                                                             one_data['educational_university'],
                                                             one_data['current_site'],
                                                             one_data['message_source'],
                                                             one_data['relative_person_links'])
    print(insert_sql)
    cur.execute(insert_sql)


def getData(file='../data/官员信息-1-8.csv'):
    all_data = list()
    with open(file, 'r') as csv_file:
        lines = csv.reader(csv_file)
        for one_line in lines:
            all_data.append(one_line)
    # csv_file = open(file, 'r')
    # lines = csv.reader(csv_file)
    # csv_file.close()
    return all_data


def makeupData(one_line):
    fabric_data = dict()
    fabric_data['id_index'] = one_line[0]
    fabric_data['officer_name'] = one_line[1]
    fabric_data['baike_url'] = one_line[2]
    fabric_data['head_image_url'] = one_line[3]
    half_message = re.sub('\(|（', '，', one_line[4])
    fabric_data['gender'] = '男' if '男' in half_message else '女'
    split_half_message = re.sub('\)|）', '', half_message).replace('。', '，').split(sep='，')
    fabric_data['place_of_birth'] = ''
    fabric_data['province'] = ''
    fabric_data['city'] = ''
    fabric_data['current_site'] = ''
    fabric_data['time_and_job'] = ''
    fabric_data['relative_person_links'] = ''
    educational_background = set()
    for mess in split_half_message:
        mess_done = mess.replace('[1]', '')
        if len(mess_done) <= 2:
            continue
        if len(fabric_data['province']) == 0:
            if '出生于' in mess_done:
                fabric_data['place_of_birth'] = mess_done[mess_done.index('出生于') + 3:]
                for province in provinces:
                    if province in mess_done:
                        fabric_data['province'] = province
                        province_pos = mess_done.find(province) + len(province)
                        city = mess_done[province_pos:].replace('省', '')
                        fabric_data['city'] = city
            elif mess_done[-1] == '人' or mess_done[:2] == '籍贯':
                fabric_data['place_of_birth'] = mess_done[2:] if '籍贯' in mess_done else mess_done[:-1]
                for province in provinces:
                    if province in mess_done:
                        fabric_data['province'] = province
                        province_pos = mess_done.find(province) + len(province)
                        city = mess_done[province_pos:] if '籍贯' in mess_done else mess_done[province_pos:-1]
                        # print(city)
                        if city.find('。') != -1:
                            fabric_data['city'] = city[:city.find('。')].replace('省', '')
                        elif city.find('1') != -1:
                            fabric_data['city'] = city[:city.find('1')].replace('省', '')
                        else:
                            fabric_data['city'] = city.replace('省', '')
                        if city.find('市') != -1:
                            fabric_data['city'] = city[:city.find('市')]
        if '学历' in mess[-2:]:
            educational_background.add(mess[:mess.index('学历') + 2])
        elif '大专' in mess:
            educational_background.add(mess[:mess.index('大专') + 2])
        elif '学士' in mess:
            educational_background.add(mess[:mess.index('学士') + 2])
        elif '硕士' in mess:
            educational_background.add(mess[:mess.index('硕士') + 2])
        elif '博士' in mess:
            educational_background.add(mess[:mess.index('博士') + 2])
        elif '研究生' in mess[-3:]:
            educational_background.add(mess[:mess.index('研究生') + 3])
        elif '学位' in mess:
            educational_background.add(mess[:mess.index('学位') + 2])
        elif '专业' in mess:
            educational_background.add(mess[:mess.index('专业') + 2])
        if '现任' in mess:
            fabric_data['current_site'] = re.sub('\[\d*-*\d*\]', '', mess.replace('现任', ''))
    if len(educational_background) != 0:
        fabric_data['educational_background'] = re.sub("\'|\{|\}", '', str(educational_background))
    else:
        fabric_data['educational_background'] = ''
    try:
        mark = half_message.index("月出生")
    except ValueError:
        try:
            mark = half_message.index("月生")
        except ValueError:
            mark = 0
    mid_messages = re.sub("\'", '', one_line[5][1:-1]).split(sep=',')
    mid_messages_dict = dict()
    for mid_message in mid_messages:
        try:
            key, value = mid_message.split(sep=':')
            mid_messages_dict[key.replace(' ', '')] = value.replace(' ', '')
        except ValueError:
            continue
    if '出生日期' in mid_messages_dict.keys():
        fabric_data['date_of_birth'] = mid_messages_dict['出生日期']
    else:
        try:
            fabric_data['date_of_birth'] = half_message[half_message.index("年") - 4 : mark + 1] if mark != 0 else ''
        except ValueError:
            fabric_data['date_of_birth'] = ''
    try:
        fabric_data['educational_university'] = mid_messages_dict['毕业院校'].replace('[1]', '')
    except KeyError:
        fabric_data['educational_university'] = ''
    fabric_data['nation'] = ''
    try:
        fabric_data['nation'] = half_message[half_message.index('族') - 1: half_message.index('族') + 1]
    except ValueError:
        try:
            fabric_data['nation'] = mid_messages_dict['民族']
        except KeyError:
            fabric_data['nation'] = ''
    try:
        fabric_data['place_of_birth'] = mid_messages_dict['出生地']
        fabric_data['officer_name'] = mid_messages_dict['中文名']
    except KeyError:
        None
    except ValueError:
        None
    fabric_data['message_source'] = '百度百科'
    time_and_job = re.sub("\'|\[|\]", '', str(process_introduce(one_line[6].replace('[1]', '').replace('[2]', '').replace('[3]', ''))))
    fabric_data['time_and_job'] = time_and_job if len(time_and_job) != 0 else ''
    if len(fabric_data['place_of_birth']) != 0:
        for super_city in super_cities:
            if super_city in fabric_data['place_of_birth']:
                fabric_data['province'] = super_city
                fabric_data['city'] = super_city
    return fabric_data


if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    for line in getData():
        print('开始结构化：' + line[0])
        data = makeupData(line)
        print('结构化数据信息：' + data['id_index'] + ' ' + data['officer_name'])
        insertMessage(Connection, data)
        print('数据库插入信息：' + data['id_index'] + ' ' + data['officer_name'])
        Connection.commit()
    Connection.close()
    print("Connection close.")
