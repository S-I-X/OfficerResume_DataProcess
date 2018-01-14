# -*- coding:utf-8 _-*-
# !/usr/bin/python


from data_structure.csv_to_postgre import getCon
edu_int_dict = {'小学': 1, '初中': 2, '高中': 3, '中专': 4, '大专': 5, '本科': 6, '大学': 6, '学士': 6, '硕士': 7, '研究生': 7, '博士': 8}
# 教育程度 1小学，2初中，3高中，4中专，5大专，6本科，7硕士，8博士

if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, current_site FROM crawler.officer_message WHERE work_place ISNULL;"
    update_sql = "UPDATE crawler.officer_message SET current_site = '{0}', work_place = '{1}' WHERE id_index = '{2}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, current_site = line
        # print(line)
        work_place = ''
        new_current_site = current_site
        if '县' in current_site:
            work_place = current_site[: current_site.find('县') + 1]
            new_current_site = current_site[current_site.find('县') + 1:]
        elif '区' in current_site:
            work_place = current_site[: current_site.find('区') + 1]
            new_current_site = current_site[current_site.find('区') + 1:]
        elif '市' in current_site:
            work_place = current_site[: current_site.find('市') + 1]
            new_current_site = current_site[current_site.find('市') + 1:]
        elif '省' in current_site:
            work_place = current_site[: current_site.find('省') + 1]
            new_current_site = current_site[current_site.find('省') + 1:]
        if len(work_place) > 10:
            if '省' in current_site:
                work_place = current_site[: current_site.find('省') + 1]
                new_current_site = current_site[current_site.find('省') + 1:]
        finish_update_sql = update_sql.format(new_current_site, work_place, id_index)
        print(finish_update_sql)
        cur.execute(finish_update_sql)
        Connection.commit()
        print(id_index, '的 工作地 信息已更新')
    cur.close()
    Connection.close()
    print('\n更新完毕')

