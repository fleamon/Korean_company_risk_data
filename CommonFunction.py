import os
import requests
from datetime import datetime
import yaml
import time
import pandas as pd
import pymysql
from pytz import timezone
import re


path = os.getcwd()
if 'ec2-user' in path:
    path += '/wooneos_koreainvestment-autotrade'

with open(f'{path}/env/config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
DART_API_KEY = _cfg['DART_API_KEY']
# headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.4459.183 Safari/537.36"}
headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0"}


def connect_to_db():
    """DB에 CONNECT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : DB CONNECT !!!!!")
    conn = pymysql.connect(host='127.0.0.1',
                           user='root',
                           password='test',
                           db='stock_Korean_by_ESG_BackData',
                           charset='utf8')
    return conn


def get_company_ceo_name():
    conn = connect_to_db()
    cursor = conn.cursor()

    company_ceo_name_list = []
    company_ceo_select_query = '''
        SELECT Korean_short_name
             , ceo_name
          FROM stock_Korean_by_ESG_BackData.Korean_all_code_info
    '''
    cursor.execute(company_ceo_select_query)
    results = cursor.fetchall()
    
    for row in results:
        if ',' in row[1]:
            for ceo_name in row[1].split(','):
                query = row[0]+'+'+ceo_name
                company_ceo_name_list.append(query)
        elif row[1] == '':
            query = row[0]
            company_ceo_name_list.append(query)
        else:
            query = row[0]+'+'+row[1]
            company_ceo_name_list.append(query)
    cursor.close()
    conn.close()

    return company_ceo_name_list


def result_delete_insert_to_db_articles_table(date_time, news_agency, portal_name, article_link, query, title, article_text):
    if "+" in query:
        company_name = query.split("+")[0]
    else:
        company_name = query

    # DB에 delete - insert
    conn = connect_to_db()
    cursor = conn.cursor()

    delete_query = f'''
        DELETE FROM Korean_company_risk_backdata.articles
            WHERE news_agency = '{news_agency}'
            AND portal_name = '{portal_name}'
            AND article_link = '{article_link}'
            AND company_name = '{company_name}'
            AND search_keyword = '{query}'
    '''
    # print (delete_query)
    cursor.execute(delete_query)

    insert_query = f'''
        INSERT INTO Korean_company_risk_backdata.articles 
        (datetime_id, news_agency, portal_name, article_link, company_name, search_keyword, title, article_text, load_date)
        VALUES 
        ('{date_time}', '{news_agency}', '{portal_name}', '{article_link}', '{company_name}'
        , '{query}', '{title}', '{article_text}', NOW())
        ON DUPLICATE KEY UPDATE 
        news_agency=VALUES(news_agency),
        portal_name=VALUES(portal_name),
        article_link=VALUES(article_link),
        company_name=VALUES(company_name),
        search_keyword=VALUES(search_keyword)
    '''
    # print (insert_query)
    cursor.execute(insert_query)
        
    conn.commit()
    cursor.close()
    conn.close()


def delete_patterns(value):
    # html태그제거 및 텍스트 다듬기
    pattern1 = '<[^>]*>'
    pattern2 = '"'
    pattern3 = "'"
    pattern4 = '\t'
    pattern5 = r'[^\w\s]'  # 이모지 패턴
    patterns = [pattern1, pattern2, pattern3, pattern4, pattern5]
    for pattern in patterns:
        value = re.sub(pattern=pattern, repl='', string=str(value))
    
    return value


def time_sleep(sec):
    print (f"{sec} seconds sleep...")
    time.sleep(sec)


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
