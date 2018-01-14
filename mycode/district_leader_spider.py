import csv

import re
import urllib
import threading

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
    div_tag = soup.find('div', class_= 'fr p2j_reports_right title_2j sjzlk')
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


def get_lemmid_and_pic_url(baike_url):
    try:
        raw_data = get_html_by_url(baike_url)
    except:
        print('baike url is error: ', baike_url)
    soup = BeautifulSoup(raw_data, 'html.parser')
    lemmid = soup.find('div', class_='lemmaWgt-promotion-rightPreciseAd').get('data-lemmaid')
    pic_div = soup.find('div', class_ = 'summary-pic')
    pic_url = ''
    if pic_div != None:
        pic_url = pic_div.find('img').get('src')
    return lemmid, pic_url


def get_person_baike_url(row):
    name, province, city, district = row
    url_list = list()
    name_url = "https://baike.baidu.com/search/word?word={0}".format(urllib.request.quote(name))
    html = get_html_by_url(name_url)
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.select('body > div.body-wrapper > div.before-content > div > ul > li')
    for item in items:
        text = item.getText()[1:]
        if province in text or city in text or district in text:
            try:
                new_name_url = "https://baike.baidu.com" + item.a['href']
                url_list.append(new_name_url)
            except AttributeError:
                url_list.append(name_url)
            except TypeError:
                url_list.append(name_url)
    return url_list


def multi_thread_fetch_part_info(index_list, csv_writer, threadLock_index_list, threadLock_csv):
    while True:
        threadLock_index_list.acquire()

        if len(index_list) < 0:
            break
        row = index_list.pop()

        threadLock_index_list.release()

        baike_url_list = get_person_baike_url(row)
        if len(baike_url_list) == 0:
            continue
        name = row[0]
        for baike_url in baike_url_list:
            lemmid, pic_url = get_lemmid_and_pic_url(baike_url)
            part_info = [lemmid, name, baike_url, pic_url]
            print(part_info)

            threadLock_csv.acquire()
            csv_writer.writerow(part_info)
            threadLock_csv.release()

def fetch_person_partinfo_from_baike(thread_num=10):
    f_index = open('../data/区级领导索引.csv', newline='', encoding='utf-8')
    csv_reader = csv.reader(f_index)
    out = open('../data/区级领导part_info.csv', 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(out)

    index_list = []

    for row in csv_reader:
        index_list.append(row)

    threadLock_csv = threading.Lock()
    threadLock_index_list = threading.Lock()

    threads = []
    for i in range(thread_num):
        t = threading.Thread(target=multi_thread_fetch_part_info, name='haha', args=(index_list, csv_writer, threadLock_index_list, threadLock_csv))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    f_index.close()
    out.close()


if __name__ == '__main__':
    # fetch_person_index_from_renminwang()
    fetch_person_partinfo_from_baike(20)
    # print(get_lemmid_and_pic_url('https://baike.baidu.com/item/%E9%AA%86%E6%96%87%E6%99%BA/1167959'))