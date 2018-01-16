# -*- coding:utf-8 _-*-
# !/usr/bin/python
import psycopg2

from data_structure.csv_to_postgre import getCon

# 处理时间，返回开始时间，结束时间，时间长度
import re


def get_split_time(re_time):
    return re.findall('\d+', re_time)


def process_time(resume_time):
    split_time = get_split_time(resume_time)
    sp_start_time = [0, 1, 1]  # 年、月、日
    sp_end_time = [0, 1, 1]
    process_tag = 0
    for item1 in split_time:
        item = item1
        if process_tag > 5:
            break
        if len(item) > 4:
            item = item[0:3]
        if len(item) == 4:
            if process_tag == 0:
                sp_start_time[0] = int(item)
                process_tag = 1
            elif process_tag <= 3:
                sp_end_time[0] = int(item)
                process_tag = 4
            else:
                break
        else:
            if process_tag == 1 or process_tag == 2:
                sp_start_time[process_tag] = int(item)
                process_tag += 1
            elif process_tag == 4 or process_tag == 5:
                sp_end_time[process_tag - 3] = int(item)
                process_tag += 1
    time_length = -1
    if sp_start_time[0] == 0 or sp_end_time[0] == 0:
        pass
    else:
        time_length = sp_end_time[0] - sp_start_time[0]
    if sp_start_time[1] > 12:
        sp_start_time[1] = 1
        sp_start_time[2] = 1
    if sp_end_time[1] > 12:
        sp_end_time[1] = 1
        sp_end_time[2] = 1
    if sp_start_time[2] > 31:
        sp_start_time[2] = 1
    if sp_end_time[2] > 31:
        sp_end_time[2] = 1
    return sp_start_time, sp_end_time, time_length


if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, time_and_job, message_source FROM crawler.officer_message WHERE head_image = '';"
    insert_sql = "INSERT INTO crawler.officer_resume (id_index, id_index_n, start_time, end_time, resume, " \
                 "message_source, work_age) VALUES ('{0}', '{1}', '{2}', '{3}'" \
                 ", '{4}', '{5}', '{6}') "
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, time_and_job, message_source = line
        print('开始往数据库写入' + id_index + '的履历信息')
        split_time_job = time_and_job.split(', ')
        # print(split_time_job, len(split_time_job))
        # try:
        for i in range(int((len(split_time_job) + 1) / 2)):
            time = split_time_job[i * 2]
            try:
                resume = split_time_job[i * 2 + 1]
            except IndexError:
                resume = ''
            id_index_n = id_index + '_' + str(i + 1)
            start_time, end_time, work_age = process_time(time)
            new_start_time = re.sub('\[|\]', '', str(start_time)).replace(', ', '-')
            new_end_time = re.sub('\[|\]', '', str(end_time)).replace(', ', '-')
            finish_insert_sql = insert_sql.format(id_index, id_index_n, new_start_time, new_end_time, resume,
                                                  message_source, work_age)
            print(finish_insert_sql)
            try:
                cur.execute(finish_insert_sql)
            except psycopg2.IntegrityError:
                print(id_index + ' 的第' + str(i + 1) + ' 条履历 已**存在**数据库')
                Connection.rollback()
                continue
            except psycopg2.DataError:
                print(id_index + ' 的第' + str(i + 1) + ' 条履历 &&&出错&&&')
                Connection.rollback()
                continue
            Connection.commit()
            print(id_index + ' 的第' + str(i + 1) + ' 条履历 已写入数据库')
        # except IndexError:
        #     None

    Connection.close()
