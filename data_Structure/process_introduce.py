#!user/bin/env python3
# -*- coding: gbk -*-
import re


def get_year_pos(text):

    all_years = re.findall('\d\d\d\d', text)


    if len(all_years) == 0:
        return

    may_pos = []
    last_pos = 0
    for item in all_years:
        pos = text[last_pos:].index(item) + last_pos
        may_pos.append(pos)
        last_pos = pos + 1
    year_pos = []
    year_pos.append(may_pos[0])
    for i in range(1, len(may_pos)):
        if may_pos[i] - may_pos[i - 1] > 12:
            year_pos.append(may_pos[i])
    return year_pos


def get_segments(text, year_pos):
    segments = []
    for i in range(len(year_pos) - 1):
        segments.append(text[year_pos[i]: year_pos[i + 1]])
    segments.append(text[year_pos[len(year_pos) - 1]:])
    return segments


def process_segment(segment):
    if segment.find('，') >= 0:
        seg_split = segment.split('，')
        return seg_split[0], seg_split[1]
    else:
        if '日后' in segment:
            mark = segment.index("日后")
            return segment[:mark], segment[mark + 2:]
        elif '日' in segment:
            mark = segment.index("日")
            return segment[:mark], segment[mark + 1:]
        elif '月后' in segment:
            mark = segment.index("月后")
            return segment[:mark], segment[mark + 2:]
        elif '月' in segment:
            mark = segment.index("月")
            return segment[:mark], segment[mark + 1:]
        else:
            mark = len(segment) - segment[::-1].index(re.findall('\d', segment[::-1])[0])
            return segment[:mark], segment[mark:]

        return segment, ''


def process_last_segment(segment):
    if segment.find('；') > 0 or segment.find('。') > 0:
        seg_split_pos = segment.find('；')
        if seg_split_pos <= 0:
            seg_split_pos = segment.find('。')
        time, work = process_segment(segment[0: seg_split_pos])
        other_info = segment[seg_split_pos+1:]
    else:
        time, work = process_segment(segment)
        other_info = ''
    return time, work, other_info


def get_divide_text(text):
    pos = []
    right_poi = 0
    left_poi = 0
    while right_poi < len(text) - 1:
        english_left_poi = text[right_poi:].find('(') + right_poi
        chinese_left_poi = text[right_poi:].find('（') + right_poi
        if english_left_poi < right_poi and chinese_left_poi < right_poi:
            break
        else:
            if english_left_poi < right_poi:
                left_poi = chinese_left_poi
            elif chinese_left_poi < right_poi:
                left_poi = english_left_poi
            else:
                left_poi = chinese_left_poi if chinese_left_poi < english_left_poi else english_left_poi

        # 找对应右括号
        right = 0
        for i in range(left_poi + 1, len(text)):
            if text[i] == '）' or text[i] == ')':
                if right == 0:
                    right_poi = i
                    pos.append([left_poi, right_poi])
                    break
                else:
                    right -= 1
            if text[i] == '（' or text[i] == '(':
                right += 1

    divide_text = []
    for item in pos:
        divide_text.append(text[item[0] + 1:item[1]])

    base = 0
    for item in pos:
        left = item[0] - base
        right = item[1] - base
        text = text[0:left] + text[right + 1:]
        base += right - left + 1
    divide_text.append(text)

    return divide_text


def process_introduce(text):
    divide_text = get_divide_text(text)  # 把括号中的内容提出来

    segments = []
    for item in divide_text:
        year_pos = get_year_pos(item)
        if year_pos == None:
            continue
        part_segments = get_segments(item, year_pos)
        segments += part_segments

    time_and_work = []
    if len(segments) == 0:
        return
    for i in range(len(segments) - 1):
        time, work = process_segment(segments[i])
        time_and_work.append([time, work])
    last_time, last_work, other_info = process_last_segment(segments[-1])
    time_and_work.append([last_time, last_work])
    time_and_work.append([other_info]) if len(other_info) > 0 else None

    return time_and_work


if __name__ == "__main__":
    text = '1981.09--1984.09，广州市机电中专学校机械制造专业学习1984.09--1987.02，广州市汽车农机公司团委干事1987.02--1988.07，广州市汽车农机工业公司团委副书记（其间：1985.08-1990.06，中国人民大学工业经济管理专业函授五年制本科学习）1988.07--1993.07，广州市机电局团委代理副书记、副书记、书记1993.07--2000.12，共青团广州市委副书记、党组成员（其间：1994.09--1997.07，广东省委党校研究生经济管理专业学习）2000.12--2003.02，共青团广州市委书记、党组书记2003.02--2004.06，广州市番禺区委副书记（市正局级，2003.05兼任区委政法委书记)2004.06--2004.08，广州市番禺区委副书记（市正局级）、政法委书记，副区长、代区长2004.08--2009.02，广州市番禺区委副书记、区长2009.02--2012.09，广州市白云区委书记、区人大常委会主任（其间：2008.10--2010.12，中山大学管理学院高级管理人员工商管理专业学习，获高级管理人员工商管理硕士学位）2012.09--2014.05，广州市萝岗区委书记，广州开发区党工委书记、管委会主任，中新广州知识城管委会主任2014.05--2014.06，广州市副市长人选、市政府党组成员2014.06--2015.05，广州市副市长、市政府党组成员[1]2015.05--2015.06，韶关市委副书记，副市长、代市长[2]2015.06--2016.01，韶关市委副书记，代市长、市政府党组书记2016.01--2017.05，韶关市委副书记，市长、市政府党组书记[3]'
    struct_data = process_introduce(text)
    if struct_data != None:
        for item in struct_data:
            print(item)
