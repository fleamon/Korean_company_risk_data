import requests
from datetime import datetime, timedelta
import time
from bs4 import BeautifulSoup
import CommonFunction as cf

news_agencies = {
    1023 : '조선일보',
    1025 : '중앙일보',
    1020 : '동아일보',
    1032 : '경향신문',
    1028 : '한겨레',
    1047 : '오마이뉴스',
    1001 : '연합뉴스',
    1003 : '뉴시스',
    1421 : '뉴스1',
    1469 : '한국일보',
    1005 : '국민일보',
    1081 : '서울신문'
}


def crawling_articles_from_keyword(query, start_date, end_date, is_public):
    """뉴스기사 DB 저장"""
    # TOBE : ceo_name 에 대해서 data cleansing 필요함
    # is_public 변수(True, False) 로 상장 / 비상장 구분
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 뉴스기사 DB 저장 !!!!!")
    try:
        print (query)
        start_datetime = datetime.strptime(start_date, "%Y.%m.%d")
        end_datetime = datetime.strptime(end_date, "%Y.%m.%d")
        # 시작 날짜부터 종료 날짜까지 하루씩 감소
        current_datetime = start_datetime
        while current_datetime >= end_datetime:
            crawling_date_id = str(current_datetime.strftime("%Y.%m.%d"))
            current_datetime -= timedelta(days=1)
            naver_news_crawler(query, crawling_date_id, '네이버')
            # daum_news_crawler(maxpage, query, daum_sort, crawling_date_id, '다음')

        cf.send_message("KOR", f"{query} 뉴스기사 DB 저장 success!")
        print(f"{query} 뉴스기사 DB 저장 successfully.")
        
    except Exception as e:
        cf.send_message("ERROR", f"뉴스기사 DB 저장 [오류 발생]{e}")
        cf.time_sleep(1)


def naver_news_crawler(query, crawling_date_id, portal_name):
    print (f'crawling_date_id = {crawling_date_id}')
    
    for agency_key, agency_value in news_agencies.items():
        # url = f"https://search.naver.com/search.naver?where=news&query={query}&pd=3&ds={crawling_date_id}&de={crawling_date_id}&mynews=1&office_type=1&news_office_checked={agency_key}"
        url = f"https://search.naver.com/search.naver?where=news&query={query}&pd=3&ds=2024.06.01&de={crawling_date_id}&mynews=1&office_type=1&news_office_checked={agency_key}"
        print ("url :", url)
        response = requests.get(url, headers=cf.headers)
        print ("response status : ", response)

        html = BeautifulSoup(response.text, "html.parser")

        print ("news_agency : ", agency_value)
        articles = html.find("ul", class_="list_news _infinite_list")
        article_list = articles.find_all("li", class_="bx")
        if len(article_list) == 0:
            cf.time_sleep(5)
            return
        
        for article in article_list:
            article_link = article.find('a', class_='info', text='네이버뉴스')['href']
            news_agency_full = article.find('a', class_='info press')
            for tag in news_agency_full.find_all('i'):
                tag.extract()
            news_agency = news_agency_full.text
            article_response = requests.get(article_link, headers=cf.headers)
            article_html = BeautifulSoup(article_response.text, "html.parser")
            title = cf.delete_patterns(article_html.find('h2', id='title_area').text)
            date_info = article_html.find('span', class_='media_end_head_info_datestamp_time _ARTICLE_DATE_TIME').text
            time_arr = date_info.split(':')[0].split()
            minute = date_info.split(':')[1]
            if '오후' in date_info:
                if time_arr[2] == '12':
                    hour = '12'
                else:
                    hour = str(int(time_arr[2]) + 12)
            else:  # 오전
                if time_arr[2] == '12':
                    hour = str(int(time_arr[2]) - 12)
                else:
                    hour = time_arr[2]
            date_time = time_arr[0] + ' ' + hour + ':' + minute
            article_text = article_html.find('article', id='dic_area')
            for tag in article_text.find_all('span', class_='end_photo_org'):
                tag.extract()
            article_text = cf.delete_patterns(article_html.find('article', id='dic_area').text)
            cf.result_delete_insert_to_db_articles_table(date_time, news_agency, portal_name, article_link, query, title, article_text)
            cf.time_sleep(1)
        cf.time_sleep(5)
        