import os
import requests
from datetime import datetime, timedelta
import yaml
import time
import pandas as pd
import pymysql
from pytz import timezone
from bs4 import BeautifulSoup
import re
from tqdm import tqdm


path = os.getcwd()
if 'ec2-user' in path:
    path += '/wooneos_koreainvestment-autotrade'

with open(f'{path}/env/config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.4459.183 Safari/537.36"}


def connect_to_db():
    """DB에 CONNECT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : DB CONNECT !!!!!")
    conn = pymysql.connect(host='127.0.0.1',
                           user='root',
                           password='test',
                           db='stock_Korean_by_ESG_BackData',
                           charset='utf8')
    return conn


def insert_Korean_all_code_info_to_DB():
    """한국 모든 종목 정보 DB에 저장"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 한국 모든 종목 정보 DB에 저장 !!!!!")
    try:
        # 한국 모든 종목 정보
        Korean_all_code_file = pd.read_csv(f'{path}/csv_file/Korean_all_code.csv', encoding='CP949')
        # print (Korean_all_code_file)

        conn = connect_to_db()
        cursor = conn.cursor()

        cursor = conn.cursor()
        truncate_query = """TRUNCATE TABLE stock_Korean_by_ESG_BackData.Korean_all_code_info"""
        cursor.execute(truncate_query)
        conn.commit()
        
        for index, row in Korean_all_code_file.iterrows():
            sql = """
            INSERT INTO stock_Korean_by_ESG_BackData.Korean_all_code_info (standard_code, short_code, Korean_name, Korean_short_name,
            English_name, stock_reg_date_id, market_type, bond_type, attach_part, stock_type, reg_price, shares_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                str(row['표준코드']),
                str(row['단축코드']),
                str(row['한글 종목명']),
                str(row['한글 종목약명']),
                str(row['영문 종목명']),
                str(row['상장일']),
                str(row['시장구분']),
                str(row['증권구분']),
                str(row['소속부']),
                str(row['주식종류']),
                str(row['액면가']),
                str(row['상장주식수'])
            )
            cursor.execute(sql, values)

        conn.commit()
        cursor.close()
        conn.close()

        send_message("KOR", "한국 모든 종목 정보 저장 success!")
        print("한국 모든 종목 정보 저장 successfully.")
        
    except Exception as e:
        send_message("ERROR", f"한국 모든 종목 정보 DB에 저장 [오류 발생]{e}")
        time.sleep(1)


def crawling_articles_from_keyword():
    """뉴스기사 DB 저장"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 뉴스기사 DB 저장 !!!!!")
    try:
        maxpage = "1"
        # querys = ["카카오+김범수"]
        # querys = ["삼성전자+이재용"]
        querys = ["카카오+김범수", "삼성전자+이재용"]
        sort = "0"
        start_date = "2024.01.01"
        end_date = "2023.01.01"
        
        for i in range(0, len(querys)):
            print (querys[i])
            start_datetime = datetime.strptime(start_date, "%Y.%m.%d")
            end_datetime = datetime.strptime(end_date, "%Y.%m.%d")
            # 시작 날짜부터 종료 날짜까지 하루씩 감소
            current_datetime = start_datetime
            while current_datetime >= end_datetime:
                crawling_date_id = str(current_datetime.strftime("%Y.%m.%d"))
                current_datetime -= timedelta(days=1)
                naver_news_crawler(maxpage, querys[i], sort, crawling_date_id) 
                print ("5 seconds sleep...")
                time.sleep(5)

        send_message("KOR", "뉴스기사 DB 저장 success!")
        print("뉴스기사 DB 저장 successfully.")
        
    except Exception as e:
        send_message("ERROR", f"뉴스기사 DB 저장 [오류 발생]{e}")
        time.sleep(1)


def naver_news_crawler(maxpage, query, sort, crawling_date_id):
    # 각 크롤링 결과 저장하기 위한 리스트 선언 
    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    result={}

    date_id = crawling_date_id.replace(".","")
    page = 1  
    maxpage =(int(maxpage)-1)*10+1  # 11= 2페이지 21=3페이지 31=4페이지  ...81=9페이지 , 91=10페이지, 101=11페이지
    
    while page <= maxpage:
        url = "https://search.naver.com/search.naver?where=news&query=" + query + "&sort="+sort+"&ds=" + crawling_date_id + "&de=" + crawling_date_id + "&nso=so%3Ar%2Cp%3Afrom" + date_id + "to" + date_id + "%2Ca%3A&start=" + str(page)
        print ("url :", url)
        original_html = requests.get(url, headers=headers)
        print ("original_html status : ", original_html)
        html = BeautifulSoup(original_html.text, "html.parser")
        
        url_info = html.select("div.group_news > ul.list_news > li div.news_area > div.news_info > div.info_group > a.info")
        for element in url_info:
            href = element.get("href")
            if "news.naver.com" in href:
                article_link.append(href)
        
        page = page + 10
    
    for naver_url in article_link:
        naver_original_html = requests.get(naver_url, headers=headers)
        print ("naver_original_html status : ", naver_original_html)
        naver_html = BeautifulSoup(naver_original_html.text, "html.parser")

        # 기사 발행일
        html_date = naver_html.select_one("div#ct> div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div > span")
        news_date = html_date.attrs['data-date-time']
        article_reg_date.append(news_date)

        # 언론사
        news_agency.append( naver_html.select_one('meta:nth-child(10)').get("content") )

        # 뉴스 제목
        title = naver_html.select_one("#ct > div.media_end_head.go_trans > div.media_end_head_title > h2")
        if title == None:
            title = naver_html.select_one("#content > div.end_ct > div > h2")

        # 뉴스 본문
        article = naver_html.select("article#dic_area")
        if article == []:
            article = naver_html.select("#articeBody")
        article = ''.join(str(article))
        
        # html태그제거 및 텍스트 다듬기
        pattern1 = '<[^>]*>'
        title = re.sub(pattern=pattern1, repl='', string=str(title))
        title = re.sub(r'[^\w\s]', '', title)
        title = title.replace("'", "")
        title = title.replace('"', "")
        title = re.sub(r'[\[\]\(\)]', '', title)
        title = re.sub(r'[\n\t]', '', title)

        article = re.sub(pattern=pattern1, repl='', string=article)
        pattern2 = """[\n\n\n\n\n// flash 오류를 우회하기 위한 함수 추가\nfunction _flash_removeCallback() {}"""
        article = article.replace(pattern2, '')
        article = re.sub(r'[^\w\s]', '', article)
        article = article.replace("'", "")
        article = article.replace('"', "")
        article = re.sub(r'[\[\]\(\)]', '', article)
        article = re.sub(r'[\n\t]', '', article)

        titles.append(title)
        article_text.append(article)
        
    result= {
            "article_reg_date" : article_reg_date
            , "article_link": article_link
            , "company_name": query.split("+")[0]
            , "news_agency" : news_agency
            , "titles" : titles
            , "article_text": article_text
            }
    df = pd.DataFrame(result)  # df로 변환
    df = df.drop_duplicates()

    # # 새로 만들 파일이름 지정
    # now = datetime.now()  # 파일이름 현 시간으로 저장하기
    # outputFileName = f'articles_from_naver_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}_{now.second}.xlsx'
    # df.to_excel('/Users/kakao/Downloads/'+outputFileName,sheet_name='sheet1')

    # DB에 insert
    conn = connect_to_db()
    cursor = conn.cursor()

    for index, row in df.iterrows():
        insert_query = f'''
            INSERT INTO stock_Korean_by_ESG_BackData.news_articles 
            (article_reg_date, article_link, company_name, news_agency, title, article_text, load_date)
            VALUES 
            ('{row["article_reg_date"]}', '{row["article_link"]}'
            , '{row["company_name"]}', '{row["news_agency"]}'
            , '{row["titles"]}', '{row["article_text"]}', NOW())
            ON DUPLICATE KEY UPDATE 
            article_link=VALUES(article_link), 
            title=VALUES(title), 
            article_text=VALUES(article_text)
        '''

        cursor.execute(insert_query)
        
    conn.commit()
    conn.close()
    

def send_message(market, msg):
    """디스코드 메세지 전송"""
    now = datetime.now()
    if market == "KOR":
        market_now = datetime.now(timezone('Asia/Seoul'))  # 한국 기준 현재 시간
    elif market == "ERROR":
        market_now = now  # 에러용

    message = {"content": f"[{market}]시간 : [{market_now.strftime('%Y-%m-%d %H:%M:%S')}] ==> {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print([time.strftime('%Y-%m-%d %H:%M:%S')], message)
