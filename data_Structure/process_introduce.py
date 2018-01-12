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
    if segment.find('��') >= 0:
        seg_split = segment.split('��')
        return seg_split[0], seg_split[1]
    else:
        if '�պ�' in segment:
            mark = segment.index("�պ�")
            return segment[:mark], segment[mark + 2:]
        elif '��' in segment:
            mark = segment.index("��")
            return segment[:mark], segment[mark + 1:]
        elif '�º�' in segment:
            mark = segment.index("�º�")
            return segment[:mark], segment[mark + 2:]
        elif '��' in segment:
            mark = segment.index("��")
            return segment[:mark], segment[mark + 1:]
        else:
            mark = len(segment) - segment[::-1].index(re.findall('\d', segment[::-1])[0])
            return segment[:mark], segment[mark:]

        return segment, ''


def process_last_segment(segment):
    if segment.find('��') > 0 or segment.find('��') > 0:
        seg_split_pos = segment.find('��')
        if seg_split_pos <= 0:
            seg_split_pos = segment.find('��')
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
        chinese_left_poi = text[right_poi:].find('��') + right_poi
        if english_left_poi < right_poi and chinese_left_poi < right_poi:
            break
        else:
            if english_left_poi < right_poi:
                left_poi = chinese_left_poi
            elif chinese_left_poi < right_poi:
                left_poi = english_left_poi
            else:
                left_poi = chinese_left_poi if chinese_left_poi < english_left_poi else english_left_poi

        # �Ҷ�Ӧ������
        right = 0
        for i in range(left_poi + 1, len(text)):
            if text[i] == '��' or text[i] == ')':
                if right == 0:
                    right_poi = i
                    pos.append([left_poi, right_poi])
                    break
                else:
                    right -= 1
            if text[i] == '��' or text[i] == '(':
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
    divide_text = get_divide_text(text)  # �������е����������

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
    text = '1981.09--1984.09�������л�����רѧУ��е����רҵѧϰ1984.09--1987.02������������ũ����˾��ί����1987.02--1988.07������������ũ����ҵ��˾��ί����ǣ���䣺1985.08-1990.06���й������ѧ��ҵ���ù���רҵ���������Ʊ���ѧϰ��1988.07--1993.07�������л������ί������ǡ�����ǡ����1993.07--2000.12�������Ź�����ί����ǡ������Ա����䣺1994.09--1997.07���㶫ʡί��У�о������ù���רҵѧϰ��2000.12--2003.02�������Ź�����ί��ǡ��������2003.02--2004.06�������з�خ��ί����ǣ������ּ���2003.05������ί����ί���)2004.06--2004.08�������з�خ��ί����ǣ������ּ���������ί��ǣ���������������2004.08--2009.02�������з�خ��ί����ǡ�����2009.02--2012.09�������а�����ί��ǡ����˴�ί�����Σ���䣺2008.10--2010.12����ɽ��ѧ����ѧԺ�߼�������Ա���̹���רҵѧϰ����߼�������Ա���̹���˶ʿѧλ��2012.09--2014.05���������ܸ���ί��ǣ����ݿ���������ί��ǡ���ί�����Σ����¹���֪ʶ�ǹ�ί������2014.05--2014.06�������и��г���ѡ�������������Ա2014.06--2015.05�������и��г��������������Ա[1]2015.05--2015.06���ع���ί����ǣ����г������г�[2]2015.06--2016.01���ع���ί����ǣ����г����������������2016.01--2017.05���ع���ί����ǣ��г����������������[3]'
    struct_data = process_introduce(text)
    if struct_data != None:
        for item in struct_data:
            print(item)
