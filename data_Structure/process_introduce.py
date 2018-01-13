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
        if right_poi <= left_poi:
            right_poi = left_poi + 1

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
    text = '1984.09—1988.07广东工学院化工系工业分析专业学习1988.07—1989.03广东省环保局监督处干部1989.03—1990.12茂名市环保局监测站挂职锻炼，任污染控制室副主任1990.12—1992.04广东省环保局监督处干部1992.04—1993.06广东省环保局监督处副科长1993.06—1995.04广东省环保局监督处科长1995.04—1999.03广东省环保局监督处副处长1999.03—2000.06广东省环保局监督处处长2000.06—2004.01广东省环保局监督管理处处长(其间：1998.09—2001.07在中央党校党员领导干部在职研究生班经济管理专业学习；2002.09—2003.01在广东省委党校中青年领导干部培训一班学习)2004.01—2004.10广东省环保局规划财务处处长2004.10—2007.04广东省环保局办公室主任(其间：2004.09—2004.12在省第五期高级公务员行政管理知识专题研究班学习)2007.04—2008.07广东省环保局环境监察分局局长(副厅级)2008.07—2010.08云浮市委常委、常务副市长(其间：2009.07—2009.08在省第五期领导干部公共管理高级培训班赴美国哥伦比亚大学学习2010.08—2011.12云浮市委常委、常务副市长，市政府党组副书记2011.12—2012.01云浮市委副书记，常务副市长，市政府党组副书记2012.01—2015.03云浮市委副书记，市社会工作委员会主任2015.03—广东省委统战部副部长，省工商联党组书记、常务副主席，省总商会常务副会长（兼）2017.8.13-继任广东省工商联（总商会）常务副主席（常务副会长）[2]2017年11月27日，当选为中华全国工商业联合会第十二届执行委员会常务委员。[1]'
    struct_data = process_introduce(text)
    if struct_data != None:
        for item in struct_data:
            print(item)
