import os
import requests
from datetime import datetime
import yaml
import time
import pandas as pd
import pymysql
from pytz import timezone


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
    """
    한국 모든 종목 정보 DB에 저장
    파일 출처는 모두 http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201040201#
    주식 > 종목정보 > 전종목 기본정보
    주식 > 세부안내 > 상장회사 상세검색
    위 두개의 csv파일
    """
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 한국 모든 종목 정보 DB에 저장 !!!!!")
    try:
        # 한국 모든 종목 정보
        Korean_all_code_file = pd.read_csv(f'{path}/csv_file/Korean_all_code.csv', encoding='CP949')
        # print (Korean_all_code_file)
        Korean_all_code_ceo_file = pd.read_csv(f'{path}/csv_file/Korean_all_code_ceo.csv', encoding='CP949')
        Korean_all_code_ceo_file['종목코드'] = Korean_all_code_ceo_file['종목코드'].astype(str)  # join에 형변환 필요
        # print (Korean_all_code_ceo_file)
        Korean_all_code_merged_df = pd.merge(Korean_all_code_file, Korean_all_code_ceo_file, left_on='한글 종목약명', right_on='종목명', how='left')
        # print (Korean_all_code_merged_df)

        conn = connect_to_db()
        cursor = conn.cursor()
        truncate_query = """TRUNCATE TABLE stock_Korean_by_ESG_BackData.Korean_all_code_info"""
        cursor.execute(truncate_query)
        conn.commit()
        
        for index, row in Korean_all_code_merged_df.iterrows():
            # print (row)
            sql = """
            INSERT INTO stock_Korean_by_ESG_BackData.Korean_all_code_info (standard_code, short_code, Korean_name, Korean_short_name, ceo_name,
            English_name, stock_reg_date_id, market_type, bond_type, attach_part, stock_type, reg_price, shares_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                str(row['표준코드']),
                str(row['단축코드']),
                str(row['한글 종목명']),
                str(row['한글 종목약명']),
                str(row['대표이사']),
                str(row['영문 종목명']),
                str(row['상장일']),
                str(row['시장구분_x']),
                str(row['증권구분']),
                str(row['소속부_x']),
                str(row['주식종류']),
                str(row['액면가_x']),
                str(row['상장주식수_x'])
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
    now = datetime.now()
    if market == "KOR":
        market_now = datetime.now(timezone('Asia/Seoul'))  # 한국 기준 현재 시간
    elif market == "ERROR":
        market_now = now  # 에러용

    message = {"content": f"[{market}]시간 : [{market_now.strftime('%Y-%m-%d %H:%M:%S')}] ==> {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print([time.strftime('%Y-%m-%d %H:%M:%S')], message)
