# -*- coding:utf-8 _-*-
# !/usr/bin/python

# from data_structure.csv_to_postgre import getCon

#处理时间，返回开始时间，结束时间，时间长度
import re


def get_split_time(time):
    return re.findall('\d+', time)


def process_time(time):
    split_time = get_split_time(time)
    start_time = [0, 0, 0]  #年、月、日
    end_time = [0, 0, 0]
    process_tag = 0
    for item in split_time:
        if process_tag > 5:
            break
        if len(item) == 4:
            if process_tag == 0:
                start_time[0] = int(item)
                process_tag = 1
            elif process_tag <= 3:
                end_time[0] = int(item)
                process_tag = 4
            else:
                break
        else:
            if process_tag == 1 or process_tag == 2:
                start_time[process_tag] = int(item)
                process_tag += 1
            elif process_tag == 4 or process_tag == 5:
                end_time[process_tag-3] = int(item)
                process_tag += 1
    time_lenth = -1
    if start_time[0] == 0 or end_time[0] == 0:
        pass
    else:
        time_lenth = end_time[0] - start_time[0]
    return start_time, end_time, time_lenth

if __name__=="__main__":
    start, end, lenth = process_time('1996.5.28-1999,4')
    print(start, end, lenth)
