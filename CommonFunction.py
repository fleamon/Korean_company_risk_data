import os
import requests
import json
import datetime
import yaml
import time
import pandas as pd
import urllib.request
import ssl
import zipfile
import pymysql
from pytz import timezone
from bs4 import BeautifulSoup as bs

path = os.getcwd()
if 'ec2-user' in path:
    path += '/wooneos_koreainvestment-autotrade'

with open(f'{path}/env/config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']


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
        print (Korean_all_code_file)

        conn = connect_to_db()
        cursor = conn.cursor()

        cursor = conn.cursor()
        truncate_query = """TRUNCATE TABLE stock_Korean_by_ESG_BackData.Korean_all_code_info"""
        cursor.execute(truncate_query)
        conn.commit()
        
        for index, row in Korean_all_code_file.iterrows():
            # print ("chk!", index)
            # print ("chk!", row)
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


def send_message(market, msg):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    if market == "KOR":
        market_now = datetime.datetime.now(timezone('Asia/Seoul'))  # 한국 기준 현재 시간
    elif market == "ERROR":
        market_now = now  # 에러용

    message = {"content": f"[{market}]시간 : [{market_now.strftime('%Y-%m-%d %H:%M:%S')}] ==> {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print([time.strftime('%Y-%m-%d %H:%M:%S')], message)

