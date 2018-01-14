import csv

import re
from bs4 import BeautifulSoup

from mycode.util import get_html_by_url, del_content_blank


def fetch_person_index(key, part_url, index_list):
    if part_url == None:
        return
    pro_url = 'http://ldzl.people.com.cn/dfzlk/front/' + part_url
    try:
        raw_data = get_html_by_url(pro_url)
    except:
        print('province url is error: ', pro_url)

    soup = BeautifulSoup(raw_data, 'html.parser')
    div_tag = soup.find('div', class_='fr p2j_reports_right title_2j sjzlk')
    city_h2_tags = div_tag.find_all('h2')
    city_div_tags = div_tag.find_all('div', class_='zlk_list')

    name_set = set()
    for i in range(len(city_h2_tags)):
        city_name = re.sub(r'\n', '', city_h2_tags[i].get_text())
        li_tags = city_div_tags[i].find_all('li')
        if len(li_tags) == 0:
            continue
        for li_tag in li_tags:
            district = del_content_blank(li_tag.find('span').get_text())
            person_name = li_tag.find('em').get_text()
            if person_name not in name_set:
                name_set.add(person_name)
                person_index = (person_name, key, city_name, district)
                print(person_index)
                index_list.append(person_index)
    return


def get_index_list():
    url = 'http://ldzl.people.com.cn/dfzlk/front/xian35.htm'
    raw_data = get_html_by_url(url)
    soup = BeautifulSoup(raw_data, 'html.parser')
    li_tags = soup.find('div', class_='fl p2j_reports_left').find_all('li')
    province_dict = {}
    for li_tag in li_tags:
        a_tag = li_tag.find('a')
        href = a_tag.get('href')
        if href == '#':
            continue
        province_dict[a_tag.get_text()] = href

    index_list = []
    for key in province_dict.keys():
        fetch_person_index(key, province_dict[key], index_list)

    return index_list


def get_person_info():
    pass


def fetch_person_index_from_renminwang():
    person_index_list = get_index_list()  # [(name, province, city, district)]
    print('person_index_list length: ', len(person_index_list))

    out = open('../data/区级领导索引.csv', 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(out)

    count = 0
    for person_index in person_index_list:
        csv_writer.writerow(list(person_index))

    out.close()


def get_person_baike_url(row):
    name, province, city, district = row
    url_list = list()
    name_url = "https://baike.baidu.com/search/word?word={0}".format(name)
    html = get_html_by_url(name_url)
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.select('body > div.body-wrapper > div.before-content > div > ul > li')
    for index, item in enumerate(items):
        text = item.getText()[1:]
        if index == 0:
            if province in text or city in text or district in text:
                url_list.append(name_url)
        else:
            if province in text or city in text or district in text:
                new_name_url = "https://baike.baidu.com" + item.a['href']
                url_list.append(new_name_url)
    return url_list


def fetch_person_partinfo_from_baike():
    f = open('../data/区级领导索引.csv', newline='', encoding='utf-8')
    csv_reader = csv.reader(f)
    part_info_list = []
    for row in csv_reader:
        pass


if __name__ == '__main__':
    # fetch_person_index_from_renminwang()
    fetch_person_partinfo_from_baike()
