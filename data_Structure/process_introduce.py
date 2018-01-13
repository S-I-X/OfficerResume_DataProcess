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
    text = '1984.09��1988.07�㶫��ѧԺ����ϵ��ҵ����רҵѧϰ1988.07��1989.03�㶫ʡ�����ּල���ɲ�1989.03��1990.12ï���л����ּ��վ��ְ����������Ⱦ�����Ҹ�����1990.12��1992.04�㶫ʡ�����ּල���ɲ�1992.04��1993.06�㶫ʡ�����ּල�����Ƴ�1993.06��1995.04�㶫ʡ�����ּල���Ƴ�1995.04��1999.03�㶫ʡ�����ּල��������1999.03��2000.06�㶫ʡ�����ּල������2000.06��2004.01�㶫ʡ�����ּල��������(��䣺1998.09��2001.07�����뵳У��Ա�쵼�ɲ���ְ�о����ྭ�ù���רҵѧϰ��2002.09��2003.01�ڹ㶫ʡί��У�������쵼�ɲ���ѵһ��ѧϰ)2004.01��2004.10�㶫ʡ�����ֹ滮���񴦴���2004.10��2007.04�㶫ʡ�����ְ칫������(��䣺2004.09��2004.12��ʡ�����ڸ߼�����Ա��������֪ʶר���о���ѧϰ)2007.04��2008.07�㶫ʡ�����ֻ������־־ֳ�(������)2008.07��2010.08�Ƹ���ί��ί�������г�(��䣺2009.07��2009.08��ʡ�������쵼�ɲ���������߼���ѵ�ะ�������ױ��Ǵ�ѧѧϰ2010.08��2011.12�Ƹ���ί��ί�������г������������鸱���2011.12��2012.01�Ƹ���ί����ǣ������г������������鸱���2012.01��2015.03�Ƹ���ί����ǣ�����Ṥ��ίԱ������2015.03���㶫ʡίͳս����������ʡ������������ǡ�������ϯ��ʡ���᳣̻�񸱻᳤���棩2017.8.13-���ι㶫ʡ�����������̻ᣩ������ϯ�����񸱻᳤��[2]2017��11��27�գ���ѡΪ�л�ȫ������ҵ���ϻ��ʮ����ִ��ίԱ�᳣��ίԱ��[1]'
    struct_data = process_introduce(text)
    if struct_data != None:
        for item in struct_data:
            print(item)
