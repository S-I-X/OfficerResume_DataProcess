

# 使用upptp代理
import csv
import json
import threading
from time import sleep
import importlib

import bs4
import re
from bs4 import BeautifulSoup

from mycode.util import get_html_by_url, del_content_blank

temp = 0


class baike_spider(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        global id_set, may_id_set, may_officer_info, counter, out, csv_writer, temp, out_other, other_csv_writer
        '''
        variable id_set: 保存所有已经写入的官员id，向文件写入新官员信息时，要保证id不在id_set中
        variable may_id_set: 保存may_officer_info中的id
        variable may_officer_info: 保存待扩展的可能是官员的信息
        variable counter: 记录进程数，第一个进程负责初始化，最后一个负责关闭文件
        variable out: 官员数据要写入的文件
        variable csv_writer: out对应的writer
        variable out_other: 非官员数据要写入的文件
        variable other_csv_writer: out_other对应的writer
        '''

        threadLock_init.acquire()

        if counter == 0:
            id_set, may_id_set, may_officer_info = self.load_id_set_and_load_may_officer_info(
                ['官员信息-初始扩展.csv', '官员信息-区级领导扩展.csv'], \
                ['人民网领导part_info.csv', '人民网领导part_info_2.csv'], \
                ['other_info.csv']
            )

            out = open('../data/官员信息-中央领导扩展.csv', 'w', newline='', encoding='utf-8')
            csv_writer = csv.writer(out, dialect='excel')

            out_other = open('../data/other_info.csv', 'w', newline='', encoding='utf-8')
            # other_csv_writer = csv.writer(out_other, dialect='excel')
        counter += 1

        self.id_set = id_set
        self.may_id_set = may_id_set
        self.may_officer_info = may_officer_info
        self.csv_writer = csv_writer
        # self.other_csv_writer = other_csv_writer

        self.out = out
        self.out_other = out_other
        self.job_dict = self.load_job_dict()

        threadLock_init.release()

        # self.may_officer_info = [['1167959', '骆文智', 'https://baike.baidu.com/item/%E9%AA%86%E6%96%87%E6%99%BA/1167959', 'https://gss3.bdstatic.com/-Po3dSag_xI4khGkpoWK1HF6hhy/baike/whfpf%3D72%2C72%2C0/sign=3c33ac9345a7d933bffdb733cb76e621/38dbb6fd5266d0163aa408af902bd40734fa35f3.jpg']]
        # self.may_officer_info.append(['211596', '万庆良', 'https://baike.baidu.com/item/%E4%B8%87%E5%BA%86%E8%89%AF/211596', 'https://gss0.bdstatic.com/94o3dSag_xI4khGkpoWK1HF6hhy/baike/whfpf%3D72%2C72%2C0/sign=494600eac4cec3fd8b6bf435b0b5e30d/7e3e6709c93d70cfcb93572bffdcd100baa12baf.jpg'])
        #
        # self.id_set = set()
        # self.id_set.add(int('1167959'))
        # self.id_set.add(int(211596))

        # self.officer_info = []   #[[lemmaId, name, url, pic_url, summary, basic-info, introduce, appoint_info, relativ_links]]

    def run(self):
        self.craw_info()

    def craw_info(self):
        '''
        爬取官员数据
        :return:
        '''
        global temp, counter

        while True:
            if len(self.may_officer_info) == 0:
                break
            officer_data = self.may_officer_info.pop(0)
            raw_data = get_html_by_url(officer_data[2])
            if raw_data == None:
                continue
            soup = BeautifulSoup(raw_data, 'html.parser')
            is_officer, filter_info = self.officer_filter(soup, officer_data)
            if not is_officer:
                print('Not Officer: ', filter_info, officer_data)
                # self.other_csv_writer.writerow(officer_data)
                continue
            self.add_officer_info(soup, officer_data)

        threadLock_init.acquire()
        counter -= 1
        if counter == 0:
            self.out.close()
            self.out_other()
        threadLock_init.release()

    def add_officer_info(self, soup, officer_data):
        '''
        对过滤器过滤成功的官员信息写入文件，但已在id_set中和含有逝世信息的官员不会添加
        :param soup: 解析后的百科网页信息
        :param officer_data: may_officer_info pop出的对应官员部分信息
        :return:
        '''
        print('开始添加新官员', officer_data)
        lemmaid = int(officer_data[0])

        relative_links = self.add_may_officer_info(officer_data[0])

        if lemmaid in self.id_set:
            print('官员已在id_set中', officer_data)
            return
        id_set.add(lemmaid)
        summary = del_content_blank(soup.find('div', class_='lemma-summary').get_text())

        basic_info = {}
        try:
            basic_info_tag = soup.find('div', class_='basic-info')
            dt_tags = basic_info_tag.find_all('dt')
            dd_tags = basic_info_tag.find_all('dd')
            for i in range(len(dt_tags)):
                key = del_content_blank(dt_tags[i].get_text())
                if key.find('逝世') >= 0:
                    print('官员已逝世', officer_data)
                    return
                value = del_content_blank(dd_tags[i].get_text())
                basic_info[key] = value
        except:
            print('No basic_info', officer_data)

        introduce = self.get_para_info(soup, '履历')
        if introduce == '':
            introduce = self.get_para_info(soup, '经历')
        if introduce == '':
            introduce = self.get_para_info(soup, '简历')
        if introduce == '':
            introduce = self.get_para_info(soup, '任职')
        if introduce == '':
            introduce = self.get_para_info(soup, '工作')
        if introduce == '':
            introduce = self.get_para_info(soup, '简介')

        appoint_info = self.get_para_info(soup, '任免')

        infor_list = officer_data + [summary, basic_info, introduce, appoint_info, relative_links]

        threadLock_csv.acquire()
        self.csv_writer.writerow(infor_list)
        threadLock_csv.release()

        global new_officer
        new_officer += 1
        print("成功添加官员：", new_officer, officer_data)

    def add_may_officer_info(self, lemmaId):
        '''
        添加官员的相关人物信息
        :param lemmaId: 要添加相关人物的官员id
        :return: 相关人物链接
        '''
        global total_count, success_count

        total_count += 1

        json_url = 'https://baike.baidu.com/wikiui/api/zhixinmap?lemmaId=' + lemmaId
        raw_data = get_html_by_url(json_url)

        if raw_data == None:
            print("获取相关链接：" + str(success_count) + '/' + str(total_count))
            print('may officer info is None')
            return []

        json_data = json.loads(str(raw_data, encoding='utf-8'))
        if not isinstance(json_data, list):
            print("获取相关链接：" + str(success_count) + '/' + str(total_count))
            print('json is false')
            return []

        relative_links = []
        for item1 in json_data:
            if item1['tipTitle'].find('人物') < 0 and item1['tipTitle'].find('学者') < 0:
                continue
            data = item1['data']
            for item in data:
                name = item['title']
                url = item['url']
                pic = item['pic']
                lemmaid = item['lemmaId']
                relative_links.append(url)

                threadLock_id_set_and_may_officer.acquire()

                if int(lemmaid) not in self.may_id_set:
                    self.may_id_set.add(int(lemmaid))
                    off_info = [lemmaid, name, url, pic]
                    self.may_officer_info.append(off_info)

                threadLock_id_set_and_may_officer.release()
                print('add may officer info Success!')

        success_count += 1
        print("获取相关链接：" + str(success_count) + '/' + str(total_count))

        return relative_links

    def find_div_begin_tag(self, soup, name):
        '''
        找到百科页面对应标题数据
        :param soup: 百科解析页面
        :param name: 标题名称
        :return: 标题数据标签
        '''
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
        '''
        拿到标题下的所有内容数据
        :param soup: 解析页面
        :param name: 标题名
        :return: 标题下内容文本
        '''
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

    def load_id_set_and_load_may_officer_info(self, officer_file_list, may_officer_file_list, other_file_list):
        '''
        加载id_set, may_id_set, may_officer_info
        :param officer_file_list: 官员文件列表，用于id_set加载
        :param may_officer_file_list: 扩展源 文件列表，用于id_set,may_id_set, may_officer_info加载
        :param other_file_list: 非官员文件列表，用于
        :return: id集合id_set, may_id_set, 待扩展官员列表may_officer_inf
        '''
        id_set = set()
        may_id_set = set()

        may_officer_info = []

        for file in officer_file_list:
            out = open('../data/' + file, newline='', encoding='utf-8')
            csv_reader = csv.reader(out)
            i = 1
            for row in csv_reader:
                id_set.add(int(row[0]))
                print("Load officer file:", file, str(i), row)
                i += 1
            out.close()

        for file in may_officer_file_list:
            out = open('../data/' + file, newline='', encoding='utf-8')
            csv_reader = csv.reader(out)
            i = 1
            for row in csv_reader:
                # id_set.add(int(row[0]))
                may_id_set.add(int(row[0]))
                may_officer_info.append(row[0:4])
                print("Load may_officer file:", file, str(i), row)
                i += 1
            out.close()

        return id_set, may_id_set, may_officer_info

    def officer_filter(self, soup, officer_data):
        '''
        判断该百科数据是否是官员
        :param soup: 链接的解析soup
        :param officer_data: may_officer_data pop出的数据，人物与soup对应
        :return: （是否是官员（bool），判断信息（str））
        '''
        # 对名字判断
        name = officer_data[1]
        if len(name) > 4:
            return False, '名字长度大于4'

        # 对summary判断
        try:
            summary = del_content_blank(soup.find('div', class_='lemma-summary').get_text())
        except:
            return False, '没有summary'

        if self.find_words(summary, ['演员','画家','相声','电影','娱乐','游戏','主持','运动员','创业','音乐','作家', \
                                     '主编','出版','教练','电视','舞蹈','英语','研发','药','医']):
            return False, 'summary包含非官员词'
        for item in self.job_dict:
            if summary.find(item) >= 0:
                return True, ''

        #对目录进行判断
        catlog_tag = soup.find('div', class_='lemmaWgt-lemmaCatalog')
        if not isinstance(catlog_tag, bs4.element.Tag):
            return False, '没找到目录'

        text = str(catlog_tag.get_text())
        if self.find_words(text, ['评论','评价','成就','获奖','研究','荣誉','创业','成果','学术','贡献','作品']):
            return False, '目录中包含非官员词'
        if not self.find_words(text, ['履历','任职','经历']):
            return False, '目录中缺少履历经历等信息'

        return True, ''

    def load_job_dict(self):
        '''
        加载用于过滤的官员名称词典
        :return: 官员名称列表
        '''
        f = open('../resource/job_dict.txt', encoding='utf-8')
        job_dict = f.readline().split(' ')
        return job_dict

    def find_words(self, text, words):
        for word in words:
            if text.find(word) >= 0:
                return True
        return False


if __name__ == '__main__':
    counter = 0
    total_count, success_count = (0, 0)
    new_officer = 0

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
