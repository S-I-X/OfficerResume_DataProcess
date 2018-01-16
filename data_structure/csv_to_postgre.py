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
    """
    :param database: 数据库名称
    :param user: 该数据库的使用者
    :param password: 密码
    :param host: 数据库地址
    :param port: 数据库链接的端口号
    :return: 返回数据库链接
    """
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    print("Connection successful.")
    return conn


def insertMessage(Conn=None, one_data=None):
    """
    :param Conn: 返回PostgreSQL的链接
    :param one_data:一条需要存放的数据，格式为字典
    :return:无返回内容
    """
    if len(one_data) == 0:
        return
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
    try:
        cur.execute(insert_sql)
        print("Insert successful !")
    except:
        print("数据库插入出错, 开始进行回滚")
        Conn.rollback()


def getData(csv_file='../data/官员信息-1-8.csv'):
    """
    将要放入数据库的csv格式的数据文件读出来存放到列表中
    :param csv_file: 需要放入数据库的csv文件名
    :return: 存放全部csv文件中数据的list
    """
    all_data = list()
    with open(csv_file, 'r') as my_csv_file:
        lines = csv.reader(my_csv_file)
        for one_line in lines:
            all_data.append(one_line)
    return all_data


def remove_dirty_word(fabric_data):
    """
    去除已经结构化数据再次清洗，目前主要对以下几种情况清洗
    1、city字段中含有 ‘省’字的，将 ‘省’ 字删除  eg: 省广州 ---> 广州
    2、每个字段中出现的[num]字符 eg: 北京市人大代表[1]、政治局常委[3]、常任理事会会员[4-6]
                                            ---> 北京市人大代表、政治局常委、常任理事会会员
    :param fabric_data: 已经结构化好的数据字典
    :return:
    """
    if len(fabric_data) == 0:
        return
    if '省' in fabric_data['city']:
        city = fabric_data['city']
        fabric_data['city'] = city[1:]
    for key, value in fabric_data.items():
        new_value = re.sub('\'|\[\d*-*\d*\]', '', value)
        fabric_data[key] = new_value
    return


def distill_edu_background(mess):
    """
    :param mess:包含学历的字段信息
    :return: 包含学历的字段集合
    """
    educational_background = set()
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
    else:
        return ''
    return educational_background


def distill_city(fabric_data, mess_done):
    """
    :param fabric_data:部分以结构化的数据，类型是字典
    :param mess_done: 每一部分去了脏字的数据
    :return: 已经抽取好的结构化好的数据
    """
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


def add_super_city(fabric_data):
    """
    对于直辖市进行添加
    :param fabric_data:基本已经结构化好的数据
    :return: 对于属于 直辖市 的跟新其省和城市都为该直辖市
    """
    if len(fabric_data['place_of_birth']) != 0:
        for super_city in super_cities:
            if super_city in fabric_data['place_of_birth']:
                fabric_data['province'] = super_city
                fabric_data['city'] = super_city

    city = fabric_data['city']
    if len(city) > 15:
        fabric_data['city'] = city[:5]


def update_dict(fabric_data, mid_messages, half_message):
    """
    更新 网站已经结构化好的数据
    :param half_message: 前半部分数据
    :param fabric_data: 通过抽取结构化的数据
    :param mid_messages: 网站自行结构化的数据
    :return: 没有网站自己结构化的数据就不返回
    """
    mid_messages_dict = dict()
    for mid_message in mid_messages:
        try:
            key, value = mid_message.split(sep=':')
            mid_messages_dict[key.replace(' ', '')] = value.replace(' ', '')
        except ValueError:
            continue
    try:
        mark = half_message.index("月出生")
    except ValueError:
        try:
            mark = half_message.index("月生")
        except ValueError:
            mark = 0
    if '出生日期' in mid_messages_dict.keys():
        fabric_data['date_of_birth'] = mid_messages_dict['出生日期']
    else:
        try:
            fabric_data['date_of_birth'] = half_message[half_message.index("年") - 4: mark + 1] if mark != 0 else ''
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
    except KeyError:
        return
    except ValueError:
        return


def makeupData(one_line):
    """
    :param one_line: 一行需要结构化的原始数据
    :return: 返回已经结构化好的数据, 类型是字典
    """
    fabric_data = dict()
    fabric_data['id_index'] = one_line[0]
    fabric_data['officer_name'] = one_line[1]
    fabric_data['baike_url'] = one_line[2]
    fabric_data['head_image_url'] = one_line[3]
    fabric_data['place_of_birth'] = ''
    fabric_data['province'] = ''
    fabric_data['city'] = ''
    fabric_data['current_site'] = ''
    fabric_data['time_and_job'] = ''
    fabric_data['relative_person_links'] = ''
    half_message = re.sub('\(|（', '，', one_line[4])
    if '男' in half_message or '男' in str(one_line[5]):
        fabric_data['gender'] = '男'
    elif '女' in half_message or '女' in str(one_line[5]):
        fabric_data['gender'] = '女'
    else:
        fabric_data['gender'] = ''
    split_half_message = re.sub('\)|）', '', half_message).replace('。', '，').split(sep='，')
    educational_background = set()
    for mess in split_half_message:
        mess_done = re.sub('\[\d*-*\d*\]', '', mess)
        if len(mess_done) <= 2:
            continue
        if len(fabric_data['province']) == 0:
            distill_city(fabric_data=fabric_data, mess_done=mess_done)
        educational_background = distill_edu_background(mess_done)
        if '现任' in mess_done:
            fabric_data['current_site'] = mess_done.replace('现任', '')
    fabric_data['educational_background'] = re.sub("\'|\{|\}", '', str(educational_background))
    update_dict(fabric_data=fabric_data, mid_messages=re.sub("\'", '', one_line[5][1:-1]).split(sep=','),
                half_message=half_message)
    time_and_job = str(process_introduce(one_line[6]))
    fabric_data['time_and_job'] = time_and_job if len(time_and_job) != 0 else ''
    add_super_city(fabric_data=fabric_data)
    remove_dirty_word(fabric_data=fabric_data)
    fabric_data['message_source'] = '百度百科'
    if len(fabric_data['officer_name']) > 15 or len(fabric_data['date_of_birth']) > 25:
        fabric_data = list()
    return fabric_data


if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    file = '../data/官员信息-1-14.csv'
    for line in getData(file):
        print('开始结构化：' + line[0])
        data = makeupData(line)
        if len(data) != 0:
            print('结构化数据信息：' + data['id_index'] + ' ' + data['officer_name'])
            try:
                insertMessage(Connection, data)
            except psycopg2.IntegrityError:
                Connection.rollback()
                continue
            print('数据库插入信息：' + data['id_index'] + ' ' + data['officer_name'])
            Connection.commit()
    Connection.close()
    print("Connection close.")
