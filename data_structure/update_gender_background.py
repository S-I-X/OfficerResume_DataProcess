# -*- coding:utf-8 _-*-
# !/usr/bin/python


from data_structure.csv_to_postgre import getCon
edu_int_dict = {'小学': 1, '初中': 2, '高中': 3, '中专': 4, '大专': 5, '本科': 6, '大学': 6, '学士': 6, '硕士': 7, '研究生': 7, '博士': 8}
# 教育程度 1小学，2初中，3高中，4中专，5大专，6本科，7硕士，8博士

if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, gender, educational_background FROM crawler.officer_message WHERE edu_int=0;"
    update_sql = "UPDATE crawler.officer_message SET gender_int = {0}, edu_int = {1} WHERE id_index = '{2}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, gender, educational_background = line
        print(line)
        gender_int = 0
        if '男' in gender:
            gender_int = 1
        elif '女' in gender:
            gender_int = 2
        else:
            gender_int = 0
        edu_int = 0
        for edu in edu_int_dict.keys():
            if edu in educational_background:
                edu_int = edu_int_dict[edu]
        finish_update_sql = update_sql.format(gender_int, edu_int, id_index)
        cur.execute(finish_update_sql)
        Connection.commit()
        print(finish_update_sql)
        print(id_index, '的 性别编号 和 学历编号 信息已更新')
    cur.close()
    Connection.close()
    print('\n更新完毕')

