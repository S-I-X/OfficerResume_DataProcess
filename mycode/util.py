import json
from urllib.request import urlopen
import urllib
from urllib import request
from time import sleep
from urllib.error import HTTPError
from urllib.error import URLError

import re


def find_all_index(arr, item):
    return [i for i, a in enumerate(arr) if a == item]


# 去掉杂七杂八的字符
def del_content_blank(s):
    clean_str = re.sub(r'\r|&nbsp|\xa0|\\xa0|\u3000|\\u3000|\\u0020|\u0020|\s|\x97', '', str(s))
    return clean_str


def get_html_by_url(url):
    req = request.Request(url)
    req.add_header("User-Agent",
                   'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36')
    req.add_header("GET", url)
    # req.add_header("Host", "baike.baidu.com")
    # req.add_header("Referer", "https://baike.baidu.com")

    flag = 1
    while flag < 10:
        try:
            html = urllib.request.urlopen(req, timeout=200)
            flag = 100
        except:
            print(url + ' is error')
            sleep(4)
        # except HTTPError:
        #     # html = urllib.request.urlopen(req)
        #     print(url + ' is error')
        #     sleep(4)
        # except URLError:
        #     # 遇到错误，等待
        #     sleep(4)

    try:
        raw_data = html.read()
    except:
        raw_data = None
    return raw_data
