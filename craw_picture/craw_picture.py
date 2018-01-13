from mycode.util import get_html_by_url


def get_pic_data(pic_url):
    raw_data = get_html_by_url(pic_url)
    print(raw_data)

if __name__ == '__main__':
    get_pic_data()