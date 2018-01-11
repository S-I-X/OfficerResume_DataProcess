import csv
import json
from time import sleep

from mycode.util import get_html_by_url

if __name__ == '__main__':
    f_in = open('../data/官员信息.csv', newline='', encoding='utf-8')
    csv_reader = csv.reader(f_in)

    f_out = open('../data/官员信息new.csv', 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(f_out, dialect='excel')


    i = 0
    for row in csv_reader:
        sleep(1)
        i += 1
        print(i)
        lemmaId = row[0]
        json_url = 'https://baike.baidu.com/wikiui/api/zhixinmap?lemmaId=' + lemmaId
        raw_data = get_html_by_url(json_url)

        if raw_data == None:
            print('may officer info is None')
            csv_writer.writerow(row)
            continue

        json_data = json.loads(str(raw_data, encoding='utf-8'))
        if not isinstance(json_data, list):
            print('json is false: ', json_data)
            csv_writer.writerow(row)
            continue

        print('find relative links Success!')
        off_infos = []
        for item1 in json_data:
            data = item1['data']
            for item in data:
                name = item['title']
                url = item['url']
                pic = item['pic']
                lemmaid = item['lemmaId']

                off_info = [lemmaid, name, url, pic]
                off_infos.append(off_info)
        print(off_infos)
        csv_writer.writerow(row + [off_infos])

    f_in.close()
    f_out.close()