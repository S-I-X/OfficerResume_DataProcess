import re


def get_year_pos(text, years):
    all_years = re.findall('\d\d\d\d', text)
    may_pos = []
    for item in all_years:
        may_pos.append(text.index(item))
    year_pos = []
    year_pos.append(may_pos[0])
    for i in range(1, len(may_pos)):
        if may_pos[i] - may_pos[i-1] > 10:
            year_pos.append(may_pos[i])
    return year_pos


def get_segments(text, year_pos):
    segments = []
    for i in range(len(year_pos)-1):
        segments.append(text[ year_pos[i] : year_pos[i+1] ])
    segments.append(text[ year_pos[len(year_pos)-1] : ])
    return segments

def process_segment(segment):
    if segment.find('，') >= 0:
        seg_split = segment.split('，')
        return seg_split[0], seg_split[1]
    else:
        return segment, ''


def process_last_segment(segment):
    if segment.find('；') > 0 or segment.find('。') > 0:
        seg_split_pos = segment.find('；')
        if seg_split_pos <= 0:
            seg_split_pos = segment.find('。')
        time, work = process_segment(segment[0: seg_split_pos])
        other_info = segment[seg_split_pos:]
    else:
        time, work = process_segment(segment)
        other_info = ''
    return time, work, other_info


def get_divide_text(text):
    pos = []
    right_poi = 0
    while right_poi < len(text)-1:
        left_poi = text[right_poi:].find('（')
        if left_poi < 0:
            break

        #找对应右括号
        right = 0
        for i in range(left_poi+1, len(text)):
            if text[i] == '）':
                if right == 0:
                    right_poi = i
                    pos.append([left_poi, right_poi])
                    break
                else:
                    right -= 1
            if text[i] == '（':
                right += 1

    divide_text = []
    for item in pos:
        divide_text.append(text[ item[0]+1:item[1] ])

    base = 0
    for item in pos:
        left = item[0] + base
        right = item[1] +base
        text = text[0:left] + text[right+1:]
        base = right - left + 1
    divide_text.append(text)

    return divide_text





def process_introduce(text):
    divide_text = get_divide_text(text)     #把括号中的内容提出来

    segments = []
    for item in divide_text:
        years = re.findall('\d\d\d\d', item)
        year_pos = get_year_pos(item, years)
        part_segments = get_segments(item, year_pos)
        segments += part_segments


    time_and_work = []
    for i in range(len(segments)-1):
        time, work = process_segment(segments[i])
        time_and_work.append([time, work])
    last_time, last_work, other_info = process_last_segment(segments[-1])
    time_and_work.append([last_time, last_work])
    time_and_work.append(other_info)

    return time_and_work

if __name__ == "__main__":
    text = 'zzzz（kk9090，（）zzz（jjj）ddd）zzzzzzzzz12zz9999bb7777cc，cccccccc'
    struct_data = process_introduce(text)
    for item in struct_data:
        print(item)