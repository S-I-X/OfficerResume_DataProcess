# 使用upptp代理
import csv
import json
import threading
from time import sleep

import bs4
from bs4 import BeautifulSoup

from mycode.util import get_html_by_url, del_content_blank

temp = 0


class baike_spider(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        global id_set, may_officer_info, counter, out, csv_writer, temp

        threadLock_init.acquire()

        if counter == 0:
            id_set, may_officer_info = self.load_id_set_and_load_may_officer_info()
            out = open('../data/官员信息.csv', 'a', newline='', encoding='utf-8')
            csv_writer = csv.writer(out, dialect='excel')
        counter += 1

        self.id_set = id_set
        self.may_officer_info = may_officer_info
        self.out = out
        self.csv_writer = csv_writer
        self.job_dict = self.load_job_dict()

        threadLock_init.release()

        # may_officer_info's format:[[lemmaId, name, url, pic_url],...]

        # self.may_officer_info = [['1167959', '骆文智', 'https://baike.baidu.com/item/%E9%AA%86%E6%96%87%E6%99%BA/1167959', 'https://gss3.bdstatic.com/-Po3dSag_xI4khGkpoWK1HF6hhy/baike/whfpf%3D72%2C72%2C0/sign=3c33ac9345a7d933bffdb733cb76e621/38dbb6fd5266d0163aa408af902bd40734fa35f3.jpg']]
        # self.may_officer_info.append(['211596', '万庆良', 'https://baike.baidu.com/item/%E4%B8%87%E5%BA%86%E8%89%AF/211596', 'https://gss0.bdstatic.com/94o3dSag_xI4khGkpoWK1HF6hhy/baike/whfpf%3D72%2C72%2C0/sign=494600eac4cec3fd8b6bf435b0b5e30d/7e3e6709c93d70cfcb93572bffdcd100baa12baf.jpg'])
        #
        # self.id_set = set()
        # self.id_set.add(int('1167959'))
        # self.id_set.add(int(211596))

        # self.officer_info = []   #[[lemmaId, name, url, pic_url, lemmaId, summary, basic-info, introduce, appoint_info]]

    def run(self):
        self.craw_info()

    def craw_info(self):
        global temp, counter
        # i = 0
        while True:
            temp = temp + 1
            print(temp)
            if len(self.may_officer_info) == 0:
                break
            officer_data = self.may_officer_info.pop(0)
            # print('may_officer_info', officer_data)
            try:
                if len(officer_data) > 1:
                    raw_data = get_html_by_url(officer_data[2])
                    soup = BeautifulSoup(raw_data, 'html.parser')

                    if not self.officer_filter(soup):
                        print('Not Officer: ', officer_data)
                        continue
                    self.add_officer_info(soup, officer_data)
                print('may_officer_info', officer_data)
                self.add_may_officer_info(officer_data[0])
            except:
                print("Error: ", officer_data)

            # i += 1
            # if i > 10000:
            #     break
        threadLock_init.acquire()
        counter -= 1
        if counter == 0:
            self.out.close()
        threadLock_init.release()

    def add_officer_info(self, soup, officer_data):

        summary = del_content_blank(soup.find('div', class_='lemma-summary').get_text())

        basic_info = {}
        try:
            basic_info_tag = soup.find('div', class_='basic-info')
            dt_tags = basic_info_tag.find_all('dt')
            dd_tags = basic_info_tag.find_all('dd')
            for i in range(len(dt_tags)):
                key = del_content_blank(dt_tags[i].get_text())
                if key.find('逝世') >= 0:
                    return
                value = del_content_blank(dd_tags[i].get_text())
                basic_info[key] = value
            print(basic_info)
            print(officer_data)
        except:
            print('No basic_info')

        introduce = self.get_para_info(soup, '履历')
        appoint_info = self.get_para_info(soup, '任免')

        infor_list = officer_data + [summary, basic_info, introduce, appoint_info]

        threadLock_csv.acquire()
        self.csv_writer.writerow(infor_list)
        threadLock_csv.release()

    def add_may_officer_info(self, lemmaId):
        json_url = 'https://baike.baidu.com/wikiui/api/zhixinmap?lemmaId=' + lemmaId
        raw_data = get_html_by_url(json_url)

        if raw_data == None:
            print('may officer info is None')
            return

        json_data = json.loads(str(raw_data, encoding='utf-8'))
        if not isinstance(json_data, list):
            print('json is false')
            return

        print('add may officer info Success!')
        for item1 in json_data:
            data = item1['data']
            for item in data:
                name = item['title']
                url = item['url']
                pic = item['pic']
                lemmaid = item['lemmaId']

                threadLock_id_set_and_may_officer.acquire()

                if int(lemmaid) not in self.id_set:
                    print("not in set")
                    self.id_set.add(int(lemmaid))
                    off_info = [lemmaid, name, url, pic]
                    self.may_officer_info.append(off_info)

                threadLock_id_set_and_may_officer.release()

    def find_div_begin_tag(self, soup, name):
        h2_tags = soup.find_all('h2', class_='title-text')
        if h2_tags == None:
            return
        h2_begin = None
        for item in h2_tags:
            if item.get_text().find(name) >= 0:
                h2_begin = item
                break
        if h2_begin == None:
            return
        div_begin = h2_begin.parent
        return div_begin

    def get_para_info(self, soup, name):
        para_info = ''
        div_begin = self.find_div_begin_tag(soup, name)
        if div_begin == None:
            return para_info
        current_tag = div_begin.next_sibling

        while True:
            if not isinstance(current_tag, bs4.element.Tag):
                current_tag = current_tag.next_sibling
                continue
            else:
                current_class = current_tag.get('class')
                if isinstance(current_class, list) and 'para' in current_class:
                    para_info += current_tag.get_text()
                    current_tag = current_tag.next_sibling
                else:
                    break
        return del_content_blank(para_info)

    def load_id_set_and_load_may_officer_info(self):
        id_set = set()
        may_officer_info = []
        out = open('../data/官员信息.csv', newline='', encoding='utf-8')
        csv_reader = csv.reader(out)
        i = 1
        for row in csv_reader:
            print(row[0])
            id_set.add(int(row[0]))
            # may_officer_info.append([str(row[0])])
            if i > 1000 and i < 1500:
                may_officer_info.append([str(row[0])])
            print("Load row: " + str(i))
            i += 1
        out.close()
        return id_set, may_officer_info

    def officer_filter(self, soup):
        summary = del_content_blank(soup.find('div', class_='lemma-summary').get_text())
        for item in self.job_dict:
            if summary.find(item) >= 0:
                return True

        if summary.find('演员') or summary.find('画家') or summary.find('相声') or summary.find('电影'):
            return False

        catlog_tag = soup.find('div', class_='lemmaWgt-lemmaCatalog')
        if not isinstance(catlog_tag, bs4.element.Tag):
            return False
        text = str(catlog_tag.get_text())
        if text.find('履历') < 0 or text.find('评论') >= 0 or text.find('评价') >= 0 or text.find('成就') >= 0 \
                or text.find('获奖') >= 0 or text.find('研究') >= 0 or text.find('荣誉') >= 0 \
                or text.find('创业') >= 0 or text.find('成果') >= 0 or text.find('学术') >= 0 \
                or text.find('贡献') >= 0 or text.find('作品') >= 0:
            return False
        return True

    def load_job_dict(self):
        f = open('../resource/job_dict.txt', encoding='utf-8')
        job_dict = f.readline().split(' ')
        return job_dict


if __name__ == '__main__':
    counter = 0

    threadLock_init = threading.Lock()
    threadLock_id_set_and_may_officer = threading.Lock()
    threadLock_csv = threading.Lock()

    threads = []

    for i in range(20):
        threads.append(baike_spider())

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print("Exiting Main Thread")
