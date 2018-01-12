import re


def get_year_pos(text, years):
    all_years = re.findall('\d\d\d\d', text)
    may_pos = []
    for item in all_years:
        may_pos.append(text.index(item))
    year_pos = []
    year_pos.append(may_pos[0])
    for i in range(1, len(may_pos)):
        if may_pos[i] - may_pos[i - 1] > 10:
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
    while right_poi < len(text) - 1:
        left_poi = text[right_poi:].find('（') + right_poi
        if left_poi < right_poi:
            break

        # 找对应右括号
        right = 0
        for i in range(left_poi + 1, len(text)):
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
        divide_text.append(text[item[0] + 1:item[1]])

    base = 0
    for item in pos:
        left = item[0] + base
        right = item[1] + base
        text = text[0:left] + text[right + 1:]
        base = right - left + 1
    divide_text.append(text)

    return divide_text


def process_introduce(text):
    divide_text = get_divide_text(text)  # 把括号中的内容提出来

    segments = []
    for item in divide_text:
        years = re.findall('\d\d\d\d', item)
        year_pos = get_year_pos(item, years)
        part_segments = get_segments(item, year_pos)
        segments += part_segments

    time_and_work = []
    for i in range(len(segments) - 1):
        time, work = process_segment(segments[i])
        time_and_work.append([time, work])
    last_time, last_work, other_info = process_last_segment(segments[-1])
    time_and_work.append([last_time, last_work])
    time_and_work.append([other_info])

    return time_and_work


if __name__ == "__main__":
    text = '1977年08月至1980年01月，在宝安县公明镇甘蔗场下乡当知青；1980年01月至1988年09月，历任深圳市财政局行财科科员、副科长、主任科员（期间：1982年09月至1984年07月，在暨南大学经济学院干部专修科财经专业学习）；1988年09月至1991年05月，任深圳市财政局行财处副处长；1991年05月至1993年11月，任深圳市财政局行财处处长（期间：1990年09月至1992年06月，在中南财经大学财经学专业硕士学位进修课程班学习，1998年获经济学硕士学位）；1993年11月至1995年04月，任深圳市财政局局长助理；1995年04月至1998年02月，任深圳市计划局副局长、党组成员；1998年02月至1998年06月，任深圳市盐田区委常委；1998年06月至1999年04月，任深圳市盐田区委常委、副区长；1999年04月至2003年03月，任潮州市委常委、副市长；2003年03月至2003年04月，任潮州市委副书记、副市长、代市长；2003年04月至2005年06月，任潮州市委副书记、市长；2005年06月至2011年08月，任潮州市委书记、市人大常委会主任（期间：2008年05月至2008年06月，在中央党校市地党政主要领导干部进修班学习）；2011年08月至2012年05月，任广东省人口计生委党组书记；2012年05月至2013年09月，任广东省人口计生委主任、党组书记；2013年09月至2016年04月，任广东省卫生计生委党组书记、副主任（期间：2014年06月至2014年07月，在国家行政学院厅局级公务员进修班学习）；2016年04月，任广东省食品药品监督管理局党组书记、局长，省食品安全委员会办公室主任。第十一届、十二届广东省委候补委员[1]。广东省第十三届人民代表大会代表[2]'
    struct_data = process_introduce(text)
    for item in struct_data:
        print(item)
