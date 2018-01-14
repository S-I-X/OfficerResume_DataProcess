import psycopg2

from mycode.util import get_html_by_url
from data_structure.csv_to_postgre import getCon


def get_pic_data(pic_url):
    raw_data = get_html_by_url(pic_url)
    return raw_data


if __name__ == '__main__':
    Connection = getCon(database='cof', user='postgres', password='postgres', host='192.168.10.6')
    select_sql = "SELECT id_index, head_image_url FROM crawler.officer_message WHERE officer_message.head_image ISNULL;"
    update_sql = "UPDATE crawler.officer_message SET head_image = {0} WHERE id_index = '{1}';"
    cur = Connection.cursor()
    cur.execute(select_sql)
    for line in cur.fetchall():
        id_index, head_image_url = line
        head_image = get_pic_data(head_image_url)
        finish_update_sql = update_sql.format(psycopg2.Binary(head_image), id_index)
        print(finish_update_sql)
        cur.execute(finish_update_sql)
        Connection.commit()
        print(id_index, "头像已存入")
    Connection.close()
