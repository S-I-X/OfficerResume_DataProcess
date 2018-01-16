# -*- coding:utf-8 _-*-
# !/usr/bin/python
import time

import re

import psycopg2

from data_structure.csv_to_postgre import getCon
from data_structure.csv_to_postgre import getData
from data_structure.csv_to_postgre import super_cities
from mycode.util import get_html_by_url

edu_int_dict = {'小学': 1, '初中': 2, '高中': 3, '中专': 4, '大专': 5, '本科': 6, '大学': 6, '学士': 6, '硕士': 7, '研究生': 7, '博士': 8}
# 教育程度 1小学，2初中，3高中，4中专，5大专，6本科，7硕士，8博士


def update_city():
    """
    该函数主要是更新将数据库officer_message表中的数据，主要是对 四个直辖市的更新，
    将 省(province) 城市(city)的字段都更新为所在地的直辖市名字
    :return: 无返回
    """
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
    cur.close()
    Connection.close()


def update_workplace():
    """
    该函数主要是更新数据库officer_message表中的数据,主要是对工作地点(work_place)字段的更新,
    将 当前工作的岗位(current_site) 中有工作地点的数据,将其中的工作地点提取出来,更新到表中
    工作地点(work_place)字段里
    :return: 无返回内容
    """
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, current_site FROM crawler.officer_message WHERE work_place ISNULL;"
    update_sql = "UPDATE crawler.officer_message SET current_site = '{0}', work_place = '{1}' WHERE id_index = '{2}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, current_site = line
        # print(line)
        work_place = ''
        new_current_site = current_site
        if '县' in current_site:
            work_place = current_site[: current_site.find('县') + 1]
            new_current_site = current_site[current_site.find('县') + 1:]
        elif '区' in current_site:
            work_place = current_site[: current_site.find('区') + 1]
            new_current_site = current_site[current_site.find('区') + 1:]
        elif '市' in current_site:
            work_place = current_site[: current_site.find('市') + 1]
            new_current_site = current_site[current_site.find('市') + 1:]
        elif '省' in current_site:
            work_place = current_site[: current_site.find('省') + 1]
            new_current_site = current_site[current_site.find('省') + 1:]
        if len(work_place) > 10:
            if '省' in current_site:
                work_place = current_site[: current_site.find('省') + 1]
                new_current_site = current_site[current_site.find('省') + 1:]
        finish_update_sql = update_sql.format(new_current_site, work_place, id_index)
        print(finish_update_sql)
        cur.execute(finish_update_sql)
        Connection.commit()
        print(id_index, '的 工作地 信息已更新')
    cur.close()
    Connection.close()
    print('\n更新完毕')


def update_age():
    """
    该函数主要是更新数据库officer_message表中的数据,主要是对年龄(age)字段的更新,
    根据 字段出生日期(date_of_birth) 计算出对应的年龄,并更新字段 年龄(age)
    :return: 无返回内容
    """
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, date_of_birth FROM crawler.officer_message WHERE age ISNULL;"
    update_sql = "UPDATE crawler.officer_message SET age = '{0}' WHERE id_index = '{1}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        officer_id, date_of_birth = line
        date = re.findall('\d\d\d\d', date_of_birth)
        print(date)
        if len(date) != 0:
            age = time.gmtime().tm_year - int(date[0])
            # print(age)
            finish_update_sql = update_sql.format(str(age), officer_id)
            cur.execute(finish_update_sql)
            Connection.commit()
    cur.close()
    Connection.close()


def update_head_image():
    """
    该函数主要是更新数据库officer_message表中的数据,主要是对头像图片(head_image)字段的更新,
    根据 数据库已存的头像链接字段(head_image_url),爬取图片并将已二进制写入数据库,更新字段
    头像图片(head_image)
    :return: 无返回内容
    """
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, head_image_url FROM crawler.officer_message WHERE officer_message.head_image ='';"
    update_sql = "UPDATE crawler.officer_message SET head_image = {0} WHERE id_index = '{1}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, head_image_url = line
        head_image = get_html_by_url(head_image_url)
        finish_update_sql = update_sql.format(psycopg2.Binary(head_image), id_index)
        print(finish_update_sql)
        cur.execute(finish_update_sql)
        Connection.commit()
        print(id_index, "的头像已存入")
    cur.close()
    Connection.close()


def update_int_gender_and_background():
    """
    该函数主要是更新数据库officer_message表中的数据,主要是对 性别代号(gender_int)字段的更新 以及
    学历代号(background_int)字段的更新：
    性别代号说明：未知：0，男：1，女：2
    学历代号说明：小学: 1, 初中: 2, 高中: 3, 中专: 4, 大专: 5, 本科: 6, 大学: 6, 学士: 6, 硕士: 7, 研究生: 7, 博士: 8
    根据 字段(gender) 和 字段(educational_background)更新
    :return: 无返回内容
    """
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, gender, educational_background FROM crawler.officer_message WHERE gender_int ISNULL;"
    update_sql = "UPDATE crawler.officer_message SET gender_int = {0}, edu_int = {1} WHERE id_index = '{2}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, gender, educational_background = line
        print(line)
        if '男' in gender:
            gender_int = 1
        elif '女' in gender:
            gender_int = 2
        else:
            gender_int = 0
        edu_int = 0
        for edu in edu_int_dict.keys():
            if edu in educational_background:
                edu_int = edu_int_dict[edu]
        finish_update_sql = update_sql.format(gender_int, edu_int, id_index)
        cur.execute(finish_update_sql)
        Connection.commit()
        print(finish_update_sql)
        print(id_index, '的 性别编号 和 学历编号 信息已更新')
    cur.close()
    Connection.close()
    print('\n更新完毕')


def update_relative_person_links(file):
    """
    该函数主要是更新数据库officer_message表中的数据,主要是对 相关人物链接(relative_person_links)字段的更新
    根据 原csv文件中 相关人物链接的数据 更新字段
    :param file: 要更新的原csv文件名 eg:'../data/官员信息-1-14.csv'
    :return: 无返回内容
    注：后期应该添加到主模块中
    """
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    update_sql = "UPDATE crawler.officer_message SET relative_person_links = '{0}' WHERE id_index = '{1}';"
    cur = Connection.cursor()
    # file = '../data/官员信息-1-14.csv'
    for line in getData(file):
        if len(line[8]) != 0:
            print('开始更新：' + line[0])
            finish_update_sql = update_sql.format(re.sub('\[|\]|\'', '', str(line[8])), line[0])
            print(finish_update_sql)
            cur.execute(finish_update_sql)
            Connection.commit()
            print(line[0], '的 相关链接 信息已更新')
    cur.close()
    Connection.close()
    print('\n更新完毕')


if __name__ == '__main__':
    update_age()
    update_city()
    update_int_gender_and_background()
    update_relative_person_links()
    update_workplace()
