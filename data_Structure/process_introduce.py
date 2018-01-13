#!user/bin/env python3
# -*- coding: gbk -*-
import re


def get_year_pos(text):

    may_all_years = re.findall('\d\d\d\d', text)
    all_years = []
    for str_year in may_all_years:
        year = int(str_year)
        if year > 1949 and year <= 2018:
            all_years.append(str(year))


    if len(all_years) == 0:
        return

    may_pos = []
    last_pos = 0
    for item in all_years:
        pos = text[last_pos:].index(item) + last_pos
        may_pos.append(pos)
        last_pos = pos + 1
    year_pos = [may_pos[0]]
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
        elif '月后' in segment:
            mark = segment.index("月后")
            return segment[:mark], segment[mark + 2:]

        elif '日' in segment:
            mark = len(segment) - segment[::-1].index(re.findall('日', segment[::-1])[0])
            return segment[:mark], segment[mark:]
        elif '月' in segment:
            mark = len(segment) - segment[::-1].index(re.findall('月', segment[::-1])[0])
            return segment[:mark], segment[mark:]
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
    text = '历任都安瑶族自治县工商局会计、所长，乡政府副乡长，工商局副局长、局长，镇党委书记，都安瑶族自治县县委常委、常务副县长，环江毛南族自治县县委副书记，河池市委副秘书长、办公室副主任、河池市接待办主任，中共凤山县委副书记、凤山人民政府县长、凤山县委书记等职务。[2]2012年09月——2014年12月任河池市人民政府党组成员、副市长。'
    struct_data = process_introduce(text)
    if struct_data != None:
        for item in struct_data:
            print(item)
