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
import websocket
import websockets
import pymysql
from pytz import timezone
from bs4 import BeautifulSoup as bs
import ETFStockList as ETFStockList

path = os.getcwd()
if 'ec2-user' in path:
    path += '/wooneos_koreainvestment-autotrade'

with open(f'{path}/env/config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']
OPTION_URL_BASE = _cfg['OPTION_URL_BASE']


def get_access_token():
    """토큰 발급"""
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    ACCESS_TOKEN = res.json()["access_token"]
    return ACCESS_TOKEN


ACCESS_TOKEN = get_access_token()


def get_Korean_total_amount_rank_bottom_20_percent():
    """한국주식 시총순위 하위 20%"""
    """관리종목, 금융사, 스팩(SPAC:우회상장)종목 제거"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 한국주식 시총순위 하위 20% !!!!!")
    try:
        # kospi, kosdaq 전체 종목
        file1 = pd.read_csv(f'{path}/csv_file/Korean_all_code.csv', encoding='CP949')
        file1 = pd.DataFrame(file1)
        file1 = file1.sort_values('시가총액').head(int(len(file1) * 0.2))
        file2 = pd.read_csv(f'{path}/csv_file/Korean_all_code_danger.csv', encoding='CP949')  # 위험종목
        file2 = pd.DataFrame(file2)
        # 모든 컬럼의 값에 'O'이 하나라도 들어있으면 row를 제외
        file2 = file2[~(file2 == 'O').any(axis=1)]
        file3 = pd.read_csv(f'{path}/csv_file/Korean_all_code_category_kospi.csv', encoding='CP949')  # 코스피 업종
        file3 = pd.DataFrame(file3)
        file4 = pd.read_csv(f'{path}/csv_file/Korean_all_code_category_kosdaq.csv', encoding='CP949')  # 코스닥 업종
        file4 = pd.DataFrame(file4)
        # 코스피, 코스닥 업종파일의 '업종명' 컬럼에 '금융'이 들어가있는 행 제외
        file1 = file1[~file1['종목코드'].isin(file3.loc[file3['업종명'].str.contains('금융'), '종목코드'])]
        file1 = file1[~file1['종목코드'].isin(file4.loc[file4['업종명'].str.contains('금융'), '종목코드'])]
        # 모든파일을 '종목코드'로 merge
        merged_df = pd.merge(file1, file2, on='종목코드', how='inner')
        candidate_dict = {}
        #  spac(우회상장의 수단 : 기업인수목적 주식회사) 종목 제외
        for idx, item in merged_df.iterrows():
            if '스팩' not in item[1]:
                candidate_dict[item[0]] = item[1]
        return candidate_dict
    except Exception as e:
        send_message("ERROR", f"한국주식 시총순위 하위 20% [오류 발생]{e}")
        time.sleep(1)


def get_Korean_stock_dict():
    """한국주식 컨센서스 4.0 이상 크롤링"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 한국주식 컨센서스 !!!!!")
    try:
        stock_code_file = f'{path}/csv_file/Korean_all_code.csv'
        read_file = pd.read_csv(stock_code_file, encoding='CP949')
        df = pd.DataFrame(read_file)

        candidate_dict = {}
        for stock_code, stock_name in zip(df['종목코드'], df['종목명']):
            page = requests.get("https://finance.naver.com/item/coinfo.naver?code=%s" % stock_code)
            soup = bs(page.text, "html.parser")
            elements = soup.select('div table tr span em')
            if len(elements) == 0 or elements[0].get_text() == 'N/A':
                continue
            else:
                if float(elements[0].get_text()) > 4.0:
                    send_message("KOR", f"{stock_name}({stock_code})'s consensus : {elements[0].get_text()}")
                    candidate_dict[stock_code] = stock_name
        return candidate_dict
    except Exception as e:
        send_message("ERROR", f"naver Crawling [오류 발생]{e}")
        time.sleep(1)


def get_USA_stock_dict(market):
    """미국주식 컨센서스 strong buy, buy"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 미국주식 컨센서스 !!!!!")
    try:
        stock_code_file = f'{path}/csv_file/%s.csv' % market
        read_file = pd.read_csv(stock_code_file)
        df = pd.DataFrame(read_file)
        candidate_dict = {}
        for stock_code, stock_name in zip(df['Symbol'], df['Name']):
            candidate_dict[stock_code] = stock_name
        return candidate_dict
    except Exception as e:
        send_message("ERROR", f"WSJ MARKETS Crawling [오류 발생]{e}")
        time.sleep(1)


def get_abnormal_date_array(date_param=None, country_param=None):
    """휴장이거나 조기종료 날짜, 시간 크롤링"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 비정상 날짜시간 크롤링 !!!!!")
    try:
        page = requests.get("https://kr.investing.com/holiday-calendar/")
        soup = bs(page.text, "html.parser")
        elements = soup.select('table tbody td')

        abnormal_val_arr = []
        for idx in range(0, len(elements) - 3, 4):
            country_name = elements[idx+1].text.strip()
            if country_name in ("한국", "미국", "스웨덴"):
                val = []
                date_id = elements[idx].text.strip()
                if len(date_id) < 1:  # 날짜가 없으면 윗줄의 날짜 찾기
                    date_idx = 4
                    while len(date_id) < 1:
                        date_id = elements[idx-date_idx].text.strip()
                        date_idx += 4
                date_id = date_id[0:4]+"-"+date_id[6:8]+"-"+date_id[10:12]
                val.append(date_id)
                val.append(country_name)
                val.append(elements[idx+3].text.strip())
                if val not in abnormal_val_arr:
                    abnormal_val_arr.append(val)  # array 에 없으면 추가

        find_abnormal = 0
        for arr in abnormal_val_arr:
            if arr[0] == date_param and arr[1] == country_param and "조기 종료" in arr[2]:
                find_abnormal += 1
                abnormal_val = arr[2].split("- ")[1].split("조기 종료")[0]
                break
            if arr[0] == date_param and arr[1] == country_param:
                find_abnormal += 1
                abnormal_val = "abnormal"
                break

        if find_abnormal > 0:
            return abnormal_val
        else:
            return "normal"
    except Exception as e:
        send_message("ERROR", f"investing Crawling [오류 발생]{e}")
        time.sleep(1)


def send_message(market, msg):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    if market == "KOR":
        market_now = datetime.datetime.now(timezone('Asia/Seoul'))  # 한국 기준 현재 시간
    elif market == "ERROR":
        market_now = now  # 에러용
    elif market == "CME":
        market_now = datetime.datetime.now(timezone('America/Chicago'))  # CME 기준 현재 시간
    else:
        market_now = datetime.datetime.now(timezone('America/New_York'))  # 뉴욕 기준 현재 시간
    message = {"content": f"[{market}]시간 : [{market_now.strftime('%Y-%m-%d %H:%M:%S')}] ==> {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print([time.strftime('%Y-%m-%d %H:%M:%S')], message)


def hashkey(datas):
    """암호화"""
    PATH = "uapi/hashkey"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        'content-Type': 'application/json',
        'appKey': APP_KEY,
        'appSecret': APP_SECRET,
    }
    res = requests.post(URL, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    return hashkey


def get_current_investor(code="005930"):
    """주식현재가 투자자 정보"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 주식현재가 투자자 정보 !!!!!")
    PATH = "uapi/domestic-stock/v1/quotations/inquire-investor"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": "FHKST01010900",
               "custtype": "P",
               }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code
    }
    res = requests.get(URL, headers=headers, params=params)
    # print("investor output ERROR DEBUG!! : ", res.json())
    res_json = res.json()['output']
    return res_json


def select_from_aws_db_kospi200_etf_investor_info_today_item():
    """AWS DB furture_option_item_info.kospi200_etf_investor_info 테이블에서 오늘 살 종목 SELECT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.kospi200_etf_investor_info 테이블에서 오늘 살 종목 SELECT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    sql = f"""
    SELECT CASE WHEN st1_sum_3 > 0 AND st2_sum_5 > 0 AND st3_sum_10 > 0
                THEN '069500'
                WHEN st1_sum_3 < 0 AND st2_sum_5 < 0 AND st3_sum_10 < 0
                THEN '252670'
                ELSE '-'
       END AS '종목코드'
     , CASE WHEN st1_sum_3 > 0 AND st2_sum_5 > 0 AND st3_sum_10 > 0
            THEN 'KODEX 200'
            WHEN st1_sum_3 < 0 AND st2_sum_5 < 0 AND st3_sum_10 < 0
            THEN 'KODEX 200 inverse 2X'
            ELSE '-'
       END AS '종목이름'
  FROM (
        SELECT st1.sum_3 AS 'st1_sum_3'
             , st2.sum_5 AS 'st2_sum_5'
             , st3.sum_10 AS 'st3_sum_10'
             , st4.sum_20 AS 'st4_sum_20'
          FROM (
                SELECT MAX(st1_date_id) AS 'date_id'
                     , SUM(st1_prsn_ntby_qty) AS 'sum_3'
                  FROM (
                        SELECT k.date_id AS 'st1_date_id'
                             , prev.prsn_ntby_qty AS 'st1_prsn_ntby_qty'
                          FROM furture_option_item_info.kospi200_etf_investor_info k
                          LEFT JOIN (
                                     SELECT date_id
                                          , prsn_ntby_qty
                                       FROM furture_option_item_info.kospi200_etf_investor_info
                                      WHERE item_code = '252670'
                                    ) prev
                                 ON prev.date_id = (
                                                    SELECT MAX(date_id)
                                                      FROM furture_option_item_info.kospi200_etf_investor_info
                                                     WHERE item_code = '252670'
                                                       AND date_id < k.date_id
                                                   )
                         WHERE k.item_code = '252670'
                         ORDER BY k.date_id DESC
                         LIMIT 3
                       ) tmp
               ) st1
         INNER JOIN (
                     SELECT MAX(st2_date_id) AS 'date_id'
                          , SUM(st2_prsn_ntby_qty) AS 'sum_5'
                       FROM (
                             SELECT k.date_id AS 'st2_date_id'
                                  , prev.prsn_ntby_qty AS 'st2_prsn_ntby_qty'
                               FROM furture_option_item_info.kospi200_etf_investor_info k
                               LEFT JOIN (
                                          SELECT date_id
                                               , prsn_ntby_qty
                                            FROM furture_option_item_info.kospi200_etf_investor_info
                                           WHERE item_code = '252670'
                                         ) prev
                                      ON prev.date_id = (
                                                         SELECT MAX(date_id)
                                                           FROM furture_option_item_info.kospi200_etf_investor_info
                                                          WHERE item_code = '252670'
                                                            AND date_id < k.date_id
                                                        )
                              WHERE k.item_code = '252670'
                              ORDER BY k.date_id DESC
                              LIMIT 5
                            ) tmp
                    ) st2
                 ON ( st1.date_id = st2.date_id
                    )
         INNER JOIN (
                     SELECT MAX(st3_date_id) AS 'date_id'
                          , SUM(st3_prsn_ntby_qty) AS 'sum_10'
                       FROM (
                             SELECT k.date_id AS 'st3_date_id'
                                  , prev.prsn_ntby_qty AS 'st3_prsn_ntby_qty'
                               FROM furture_option_item_info.kospi200_etf_investor_info k
                               LEFT JOIN (
                                          SELECT date_id
                                               , prsn_ntby_qty
                                            FROM furture_option_item_info.kospi200_etf_investor_info
                                           WHERE item_code = '252670'
                                         ) prev
                                      ON prev.date_id = (
                                                         SELECT MAX(date_id)
                                                           FROM furture_option_item_info.kospi200_etf_investor_info
                                                          WHERE item_code = '252670'
                                                            AND date_id < k.date_id
                                                        )
                              WHERE k.item_code = '252670'
                              ORDER BY k.date_id DESC
                              LIMIT 10
                            ) tmp
                    ) st3
                 ON ( st1.date_id = st3.date_id
                    )
         INNER JOIN (
                     SELECT MAX(st4_date_id) AS 'date_id'
                          , SUM(st4_prsn_ntby_qty) AS 'sum_20'
                       FROM (
                             SELECT k.date_id AS 'st4_date_id'
                                  , prev.prsn_ntby_qty AS 'st4_prsn_ntby_qty'
                               FROM furture_option_item_info.kospi200_etf_investor_info k
                               LEFT JOIN (
                                          SELECT date_id
                                               , prsn_ntby_qty
                                            FROM furture_option_item_info.kospi200_etf_investor_info
                                           WHERE item_code = '252670'
                                         ) prev
                                      ON prev.date_id = (
                                                         SELECT MAX(date_id)
                                                           FROM furture_option_item_info.kospi200_etf_investor_info
                                                          WHERE item_code = '252670'
                                                            AND date_id < k.date_id
                                                        )
                              WHERE k.item_code = '252670'
                              ORDER BY k.date_id DESC
                              LIMIT 20
                            ) tmp
                    ) st4
                 ON ( st1.date_id = st4.date_id
                    )
       ) tmp
    """
    # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows


def insert_to_aws_db_kospi200_etf_investor_info(item_code, item_name, investor_info):
    """AWS DB furture_option_item_info.kospi200_etf_investor_info 테이블에 INSERT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.kospi200_etf_investor_info 테이블에 INSERT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    sql = f"""
    DELETE FROM furture_option_item_info.kospi200_etf_investor_info
          WHERE date_id = '{investor_info['stck_bsop_date']}'
            AND item_code = '{item_code}'
    """
    # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
    cursor.execute(sql)
    conn.commit()
    if investor_info['prdy_vrss_sign'] == '1':
        prdy_vrss_sign = 'max up'
    elif investor_info['prdy_vrss_sign'] == '2':
        prdy_vrss_sign = 'up'
    elif investor_info['prdy_vrss_sign'] == '3':
        prdy_vrss_sign = 'flat'
    elif investor_info['prdy_vrss_sign'] == '4':
        prdy_vrss_sign = 'max down'
    elif investor_info['prdy_vrss_sign'] == '5':
        prdy_vrss_sign = 'down'
    # 집계전에는 안나오는 듯
    if investor_info['prsn_ntby_qty'] == '':
        prsn_ntby_qty = 0
    else:
        prsn_ntby_qty = investor_info['prsn_ntby_qty']
    if investor_info['frgn_ntby_qty'] == '':
        frgn_ntby_qty = 0
    else:
        frgn_ntby_qty = investor_info['frgn_ntby_qty']
    if investor_info['orgn_ntby_qty'] == '':
        orgn_ntby_qty = 0
    else:
        orgn_ntby_qty = investor_info['orgn_ntby_qty']
    sql = f"""
        INSERT INTO furture_option_item_info.kospi200_etf_investor_info (
            date_id
            , item_code
            , item_name
            , stck_clpr
            , prdy_vrss
            , prdy_vrss_sign
            , prsn_ntby_qty
            , frgn_ntby_qty
            , orgn_ntby_qty
            , load_dttm
        )
        VALUES (
            '{investor_info['stck_bsop_date']}', '{item_code}', '{item_name}'
            , {investor_info['stck_clpr']}, {investor_info['prdy_vrss']}, '{prdy_vrss_sign}'
            , {prsn_ntby_qty}, {frgn_ntby_qty}, {orgn_ntby_qty},
            DATE_ADD(now(), INTERVAL 9 HOUR)
        )
    """
    # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
    cursor.execute(sql)
    conn.commit()


def get_balance(market):
    """현금 잔고조회"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 현금 잔고 !!!!!")
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": "TTTC8908R",
               "custtype": "P",
               }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": "005930",
        "ORD_UNPR": "65500",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "Y",
        "OVRS_ICLD_YN": "Y"
    }
    res = requests.get(URL, headers=headers, params=params)
    cash = res.json()['output']['nrcvb_buy_amt']
    return int(cash)


def get_current_price(market="KOR", code="005930"):
    """현재가 조회"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 현재가 !!!!!")
    if market == "KOR":
        PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
        }
    else:  # 미국주식
        PATH = "uapi/overseas-price/v1/quotations/price"
        tr_id = "HHDFS00000300"
        params = {
            "AUTH": "",
            "EXCD": market,
            "SYMB": code,
        }
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": tr_id}

    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params)

    if market == "KOR":
        return int(res.json()['output']['stck_prpr'])
    else:
        if res.json()['output']['last'] == '':
            return 0
        else:
            return float(res.json()['output']['last'])


def get_target_price(market="KOR", code="005930"):
    """변동성 돌파 전략으로 매수 목표가 조회"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 목표가 !!!!!")
    if market == "KOR":
        PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
        tr_id = "FHKST01010400"
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
            "fid_org_adj_prc": "1",
            "fid_period_div_code": "D"
        }
    else:
        PATH = "uapi/overseas-price/v1/quotations/dailyprice"
        tr_id = "HHDFS76240000"
        params = {
            "AUTH": "",
            "EXCD": market,
            "SYMB": code,
            "GUBN": "0",
            "BYMD": "",
            "MODP": "0"
        }
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": tr_id}

    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params)

    if market == "KOR":
        stck_oprc = int(res.json()['output'][0]['stck_oprc'])  # 오늘 시가
        stck_hgpr = int(res.json()['output'][1]['stck_hgpr'])  # 전일 고가
        stck_lwpr = int(res.json()['output'][1]['stck_lwpr'])  # 전일 저가
        target_price = stck_oprc + (stck_hgpr - stck_lwpr) * 0.5
        return target_price
    else:
        if res.json()['output2'][0]['open'] == '' and res.json()['output2'][1]['high'] == '' and res.json()['output2'][1]['low'] == '':
            return 8888888888888
        else:
            stck_oprc = float(res.json()['output2'][0]['open'])  # 오늘 시가
            stck_hgpr = float(res.json()['output2'][1]['high'])  # 전일 고가
            stck_lwpr = float(res.json()['output2'][1]['low'])  # 전일 저가
            target_price = stck_oprc + (stck_hgpr - stck_lwpr) * 0.5
            return target_price


def get_inquire_balance(market="KOR"):
    """잔고조회 관련 API"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 주식 잔고조회 관련 API !!!!!")
    if market == "KOR":
        PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "TTTC8434R"
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
    else:
        PATH = "uapi/overseas-stock/v1/trading/inquire-balance"
        tr_id = "JTTT3012R"
        params = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "OVRS_EXCG_CD": "NASD",
            "TR_CRCY_CD": "USD",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": ""
        }
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": tr_id,
               "custtype": "P",
               }

    URL = f"{URL_BASE}/{PATH}"
    res = requests.get(URL, headers=headers, params=params)
    return res


def get_stock_balance(market="KOR"):
    """주식 잔고조회"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 주식 잔고 !!!!!")
    res = get_inquire_balance(market)

    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    stock_dict = {}
    etf_total_amount = 0
    etf_evaluation_amount = 0
    stock_evaluation_amount = 0
    send_message(market, f"====주식 보유잔고====")
    for stock in stock_list:
        if market == "KOR" and int(stock['hldg_qty']) > 0:
            stock_dict[stock['pdno']] = stock['hldg_qty']
            if stock['pdno'] in ETFStockList.ETF_symbol_list.keys():
                etf_total_amount += int(stock['hldg_qty']) * int(stock['prpr'])  # etf 총 평가금액
                etf_evaluation_amount += int(stock['evlu_pfls_amt'])  # etf 손익
            else:  # stock['pdno'] not in ETFStockList.ETF_symbol_list.keys()
                stock_evaluation_amount += int(stock['evlu_pfls_amt'])
                print(stock['pdno'], " : ", stock['prdt_name'])
                print(int(stock['evlu_pfls_amt']))
            send_message(market, f"{stock['prdt_name']}({stock['pdno']}): {stock['hldg_qty']}주 ==> 매입평균: {int(stock['pchs_avg_pric'].split('.')[0])}원, 현재가: {int(stock['prpr'])}원, 손익: {stock['evlu_pfls_amt']}원 ({stock['evlu_pfls_rt']}%)")
            time.sleep(0.1)
        elif market != "KOR" and int(stock['ovrs_cblc_qty']) > 0:
            stock_dict[stock['ovrs_pdno']] = stock['ovrs_cblc_qty']
            send_message(market, f"{stock['ovrs_item_name']}({stock['ovrs_pdno']}): {stock['ovrs_cblc_qty']}주 ==> 매입평균: ${stock['pchs_avg_pric']}, 현재가: ${stock['now_pric2']}, 손익: ${stock['frcr_evlu_pfls_amt']} ({stock['evlu_pfls_rt']}%)")
            time.sleep(0.1)
    if market == "KOR":
        send_message(market, f"======= ETF 손익! =======")
        send_message(market, f"ETF 평가 금액: {etf_total_amount}원")
        time.sleep(0.1)
        send_message(market, f"ETF 손익 합계: {etf_evaluation_amount}원")
        time.sleep(0.1)
        send_message(market, f"=== ETF는 제외하고 출력! ===")
        etf_amount = int(evaluation[0]['scts_evlu_amt']) - etf_total_amount
        send_message(market, f"주식 평가 금액: {etf_amount}원")
        time.sleep(0.1)
        send_message(market, f"주식 손익 합계: {stock_evaluation_amount}원")
        time.sleep(0.1)
        send_message(market, f"========================")
        send_message(market, f"총 평가 금액: {evaluation[0]['tot_evlu_amt']}원")
        time.sleep(0.1)
    else:
        send_message(market, f"주식 평가 금액: ${evaluation['tot_evlu_pfls_amt']}")
        time.sleep(0.1)
        send_message(market, f"평가 손익 합계: ${evaluation['ovrs_tot_pfls']}")
        time.sleep(0.1)
    send_message(market, f"=================")
    return stock_dict


def get_stock_balance_for_check(market="KOR", nyse_symbol_list=[], amex_symbol_list=[]):
    """주식 잔고 조회하여 필요할 경우 익절 또는 손절"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 익절 또는 손절 !!!!!")
    res = get_inquire_balance(market)

    stock_list = res.json()['output1']
    need_sell_stock = []
    for stock in stock_list:
        if market == "KOR":
            if stock['pdno'] in ETFStockList.ETF_symbol_list.keys():
                continue
            if float(stock['evlu_pfls_rt']) > 5.0 or float(stock['evlu_pfls_rt']) < -3.0:
                send_message(market, f"익절!!!!! or 손절!!!!! {stock['prdt_name']}({stock['pdno']}): {stock['hldg_qty']}주 ==> 매입평균: {int(stock['pchs_avg_pric'].split('.')[0])}원, 현재가: {int(stock['prpr'])}원, 손익: {stock['evlu_pfls_amt']}원 ({stock['evlu_pfls_rt']}%)")
                sell("KOR", stock['pdno'], stock['hldg_qty'], "0")
                need_sell_stock.append(stock['pdno'])
        else:  # market != "KOR"
            market1 = "NASD"
            if stock['ovrs_pdno'] in nyse_symbol_list:
                market1 = "NYSE"
            if stock['ovrs_pdno'] in amex_symbol_list:
                market1 = "AMEX"
            if float(stock['evlu_pfls_rt']) > 5.0 or float(stock['evlu_pfls_rt']) < -3.0:
                send_message(market, f"익절!!!!! or 손절!!!!! {stock['ovrs_item_name']}({stock['ovrs_pdno']}): {stock['ovrs_cblc_qty']}주 ==> 매입평균: ${stock['pchs_avg_pric']}, 현재가: ${stock['now_pric2']}, 손익: ${stock['frcr_evlu_pfls_amt']} ({stock['evlu_pfls_rt']}%)")
                sell(market1, stock['ovrs_pdno'], stock['ovrs_cblc_qty'], "0")
                need_sell_stock.append(stock['ovrs_pdno'])
    return need_sell_stock


def buy(market="KOR", code="005930", qty="1", price="0"):
    """국내, 미국 : 주식 지정가 매수"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 매수 !!!!!")
    if str(price) == "0":
        ORD_DVSN = "01"  # 시장가
    else:
        ORD_DVSN = "00"  # 지정가
    if market == "KOR":
        PATH = "uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "TTTC0802U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code[0],
            "ORD_DVSN": ORD_DVSN,
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }
    else:
        PATH = "uapi/overseas-stock/v1/trading/order"
        tr_id = "JTTT1002U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "OVRS_EXCG_CD": market,
            "PDNO": code[0],
            "ORD_DVSN": ORD_DVSN,
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": f"{round(float(price),2)}",
            "ORD_SVR_DVSN_CD": "0"
        }
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": tr_id,
               "custtype": "P",
               "hashkey": hashkey(data)
               }

    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(data))

    if res.json()['rt_cd'] == '0':
        send_message(market, f"{code[1]} (={code[0]}) [매수 성공]{str(res.json())}")
        return True
    else:
        send_message(market, f"{code[1]} (={code[0]}) [매수 실패]{str(res.json())}")
        return False


def sell(market="KOR", code="005930", qty="1", price="0"):
    """국내, 미국 : 주식 지정가 매도"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 매도 !!!!!")
    if str(price) == "0":
        ORD_DVSN = "01"  # 시장가
    else:
        ORD_DVSN = "00"  # 지정가
    if market == "KOR":
        PATH = "uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "TTTC0801U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": ORD_DVSN,
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }
    else:
        PATH = "uapi/overseas-stock/v1/trading/order"
        tr_id = "JTTT1006U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "OVRS_EXCG_CD": market,
            "PDNO": code,
            "ORD_DVSN": ORD_DVSN,
            "ORD_QTY": str(qty),
            "OVRS_ORD_UNPR": f"{round(float(price),2)}",
            "ORD_SVR_DVSN_CD": "0"
        }
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": tr_id,
               "custtype": "P",
               "hashkey": hashkey(data)
               }

    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(data))

    if res.json()['rt_cd'] == '0':
        send_message(market, f"[매도 성공]{str(res.json())}")
        return True
    else:
        send_message(market, f"[매도 실패]{str(res.json())}")
        return False


def get_exchange_rate():
    """환율 조회"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 환율 조회 !!!!!")
    PATH = "uapi/overseas-stock/v1/trading/inquire-present-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": "CTRP6504R"}
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "OVRS_EXCG_CD": "NASD",
        "WCRC_FRCR_DVSN_CD": "01",
        "NATN_CD": "840",
        "TR_MKET_CD": "01",
        "INQR_DVSN_CD": "00"
    }
    res = requests.get(URL, headers=headers, params=params)
    if len(res.json()['output2']) > 0:
        exchange_rate = float(res.json()['output2'][0]['frst_bltn_exrt'])
    return exchange_rate


def connect_to_aws_db():
    """AWS DB에 CONNECT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB CONNECT !!!!!")
    conn = pymysql.connect(host='13.230.62.29',
                           user='grafana',
                           password='grafana123',
                           db='furture_option_item_info',
                           charset='utf8')
    return conn


##############################
##### 국내선물 옵션 관련 함수 #####
##############################


def get_domestic_future_master_dataframe(item_code_list):
    """국내선물옵션 정보 목록"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 국내선물옵션 정보 목록 !!!!!")
    # download file
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "Downloading...")

    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/fo_idx_code_mts.mst.zip", f"{path}/fo_idx_code_mts.mst.zip")
    os.chdir(path)

    fo_idx_code_zip = zipfile.ZipFile('fo_idx_code_mts.mst.zip')
    fo_idx_code_zip.extractall()
    fo_idx_code_zip.close()
    file_name = f"{path}/fo_idx_code_mts.mst"

    with open(file_name, mode="r", encoding="cp949") as f:
        item_dict = {}
        for row in f:
            # print ([time.strftime('%Y-%m-%d %H:%M:%S')], row)
            if row.split('|')[1] in item_code_list:
                item_dict[row.split('|')[1].strip()] = row.split('|')[-1].strip() + '-' + row.split('|')[3].strip()

    """
    # for excel export
    columns = ['상품종류','단축코드','표준코드',' 한글종목명',' ATM구분',
               ' 행사가',' 월물구분코드',' 기초자산 단축코드',' 기초자산 명']
    df=pd.read_table(file_name, sep='|',encoding='cp949',header=None)
    df.columns = columns
    df.to_excel('fo_idx_code_mts.xlsx',index=False)  # 현재 위치에 엑셀파일로 저장
    """

    return item_dict


async def get_Korean_futures_option_realtime_trade_price(item_dict):
    """국내선물옵션 실시간 체결가 조회"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 국내선물옵션 실시간 체결가 !!!!!")
    approval_key = get_approval()
    item_code_list = item_dict.keys()

    senddata_list = []
    for item_code in item_code_list:
        senddata = '{"header":{"approval_key": "%s","custtype":"P","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id":"H0IFCNT0","tr_key":"%s"}}}' % (approval_key, item_code)
        senddata_list.append(senddata)

    while True:
        async with websockets.connect(OPTION_URL_BASE, ping_interval=30) as websocket:
            for senddata in senddata_list:
                await websocket.send(senddata)
                time.sleep(0.5)
                print([time.strftime('%Y-%m-%d %H:%M:%S')], f"Input Command is :{senddata}")
            while True:
                try:
                    data = await websocket.recv()
                    time.sleep(0.5)
                    # 정제되지 않은 Request / Response 출력
                    print([time.strftime('%Y-%m-%d %H:%M:%S')], f"Recev Command is :{data}")
                    if data[0] == '0':
                        # 수신데이터가 실데이터 이전은 '|'로 나뉘어져있어 split
                        recvstr = data.split('|')
                        print([time.strftime('%Y-%m-%d %H:%M:%S')], "#### 국내선물옵션체결 ####")
                        data_cnt = int(recvstr[2])  # 체결데이터 개수
                        stockspurchase_Korean_futures(data_cnt, recvstr[3])
                except websockets.ConnectionClosed:
                    continue


def stockspurchase_Korean_futures(data_cnt, data):
    """국내선물옵션 체결처리 출력라이브러리"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 국내선물옵션 체결처리 출력라이브러리 !!!!!")
    # menulist = "선물단축종목코드|영업시간|선물전일대비|전일대비부호|선물전일대비율|선물현재가|선물시가2|선물최고가|선물최저가|최종거래량|누적거래량|누적거래대금|HTS이론가|>시장베이시스|괴리율|근월물약정가|원월물약정가|스프레드1|HTS미결제약정수량|미결제약정수량증감|시가시간|시가2대비현재가부호|시가대비지수현재가|최고가시간|최고가대비현재가부호|최고가대비지수현재가|최저가시간|최저가대비현재가부호|최저가대비지수현재가|매수2비율|체결강도|괴리도|미결제약정직전수량증감|이론베이시스|선>물매도호가1|선물매수호가1|매도호가잔량1|매수호가잔량1|매도체결건수|매수체결건수|순매수체결건수|총매도수량|총매수수량|총매도호가잔량|총매수호가잔량|전일거래>량대비등락율|협의대량거래량|실시간상한가|실시간하한가|실시간가격제한구분"
    # menustr = menulist.split('|')
    pValue = data.split('^')
    # print ([time.strftime('%Y-%m-%d %H:%M:%S')], "chk pValue!! : ", pValue)
    i = 0
    for cnt in range(data_cnt):  # 넘겨받은 체결데이터 개수만큼 print 한다
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], "### [%d / %d]" % (cnt + 1, data_cnt))
        insert_to_aws_db_futures_option_Korean_realtime_info(pValue)
        # for menu in menustr:
        #     print ([time.strftime('%Y-%m-%d %H:%M:%S')], "%-13s[%s]" % (menu, pValue[i]))
        #     i += 1


def insert_to_aws_db_futures_option_Korean_realtime_info(pValue):
    """AWS DB furture_option_item_info.futures_option_Korean_realtime_info 테이블에 INSERT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.futures_option_Korean_realtime_info 테이블에 INSERT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO furture_option_item_info.futures_option_Korean_realtime_info (
            date_id
            , item_code
            , bsop_hour
            , futs_prdy_vrss
            , prdy_vrss_sign
            , futs_prdy_ctrt
            , futs_prpr
            , futs_oprc
            , futs_hgpr
            , futs_lwpr
            , last_cnqn
            , acml_vol
            , acml_tr_pbmn
            , hts_thpr
            , mrkt_basis
            , dprt
            , nmsc_fctn_stpl_prc
            , fmsc_fctn_stpl_prc
            , spead_prc
            , hts_otst_stpl_qty
            , otst_stpl_qty_icdc
            , oprc_hour
            , oprc_vrss_prpr_sign
            , oprc_vrss_nmix_prpr
            , hgpr_hour
            , hgpr_vrss_prpr_sign
            , hgpr_vrss_nmix_prpr
            , lwpr_hour
            , lwpr_vrss_prpr_sign
            , lwpr_vrss_nmix_prpr
            , shnu_rate
            , cttr
            , esdg
            , otst_stpl_rgbf_qty_icdc
            , thpr_basis
            , futs_askp1
            , futs_bidp1
            , askp_rsqn1
            , bidp_rsqn1
            , seln_cntg_csnu
            , shnu_cntg_csnu
            , ntby_cntg_csnu
            , seln_cntg_smtn
            , shnu_cntg_smtn
            , total_askp_rsqn
            , total_bidp_rsqn
            , prdy_vol_vrss_acml_vol_rate
            , dscs_bltr_acml_qty
            , dynm_mxpr
            , dynm_llam
            , dynm_prc_limt_yn
            , load_dttm
        )
        VALUES (
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 9 HOUR), '%%Y%%m%%d'),
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            DATE_ADD(now(), INTERVAL 9 HOUR)
        )
    """
    for idx in range(0, len(pValue), 50):
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql, (pValue[idx:idx+50]))
        conn.commit()


##############################
##### 해외선물 옵션 관련 함수 #####
##############################


def get_overseas_future_master_dataframe(item_code_list):
    """해외선물옵션 정보 목록"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 해외선물옵션 정보 목록 !!!!!")
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/ffcode.mst.zip", f"{path}/ffcode.mst.zip")
    os.chdir(path)

    nas_zip = zipfile.ZipFile(f'{path}/ffcode.mst.zip')
    nas_zip.extractall()
    nas_zip.close()

    file_name = "ffcode.mst"
    columns = ['종목코드', '서버자동주문 가능 종목 여부', '서버자동주문 TWAP 가능 종목 여부', '서버자동 경제지표 주문 가능 종목 여부',
               '필러', '종목한글명', '거래소코드 (ISAM KEY 1)', '품목코드 (ISAM KEY 2)', '품목종류', '출력 소수점', '계산 소수점',
               '틱사이즈', '틱가치', '계약크기', '가격표시진법', '환산승수', '최다월물여부 0:원월물 1:최다월물',
               '최근월물여부 0:원월물 1:최근월물', '스프레드여부', '스프레드기준종목 LEG1 여부', '서브 거래소 코드']
    df = pd.DataFrame(columns=columns)
    ridx = 1
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "Downloading...")
    with open(file_name, mode="r", encoding="cp949") as f:
        item_dict = {}
        for row in f:
            a = row[:32]              # 종목코드
            f = row[82:107].rstrip()  # 종목명
            """
            # excel
            b = row[32:33].rstrip()   # 서버자동주문 가능 종목 여부
            c = row[33:34].rstrip()   # 서버자동주문 TWAP 가능 종목 여부
            d = row[34:35]            # 서버자동 경제지표 주문 가능 종목 여부  
            e = row[35:82].rstrip()   # 필러
            g = row[-92:-82]          # 거래소코드 (ISAM KEY 1)  
            h = row[-82:-72].rstrip() # 품목코드 (ISAM KEY 2)
            i = row[-72:-69].rstrip() # 품목종류
            j = row[-69:-64]          # 출력 소수점  
            k = row[-64:-59].rstrip() # 계산 소수점
            l = row[-59:-45].rstrip() # 틱사이즈
            m = row[-45:-31]          # 틱가치
            n = row[-31:-21].rstrip() # 계약크기 
            o = row[-21:-17].rstrip() # 가격표시진법
            p = row[-17:-7]          # 환산승수
            q = row[-7:-6].rstrip() # 최다월물여부 0:원월물 1:최다월물
            r = row[-6:-5].rstrip() # 최근월물여부 0:원월물 1:최근월물
            s = row[-5:-4].rstrip() # 스프레드여부
            t = row[-4:-3].rstrip() # 스프레드기준종목 LEG1 여부 Y/N
            u = row[-3:].rstrip() # 서브 거래소 코드
            
            # excel
            df.loc[ridx] = [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u]
            ridx += 1
            """

            if a.strip() in item_code_list:
                item_dict[a.strip()] = f.strip()

    # excel
    # df.to_excel('ffcode.xlsx',index=False)  # 현재 위치에 엑셀파일로 저장
    # print ([time.strftime('%Y-%m-%d %H:%M:%S')], df)
    return item_dict


def get_approval():
    """웹소켓 접속키 발급"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 웹소켓 접속키 발급 !!!!!")
    PATH = "oauth2/Approval"
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": APP_KEY,
            "secretkey": APP_SECRET}
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    approval_key = res.json()["approval_key"]
    return approval_key


def insert_to_aws_db_futures_option_realtime_info(pValue):
    """AWS DB furture_option_item_info.futures_option_realtime_info 테이블에 INSERT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.futures_option_realtime_info 테이블에 INSERT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO furture_option_item_info.futures_option_realtime_info (
            item_code,
            date_id,
            market_start_date_id,
            market_start_datetime_id,
            market_end_date_id,
            market_end_datetime_id,
            yesterday_end_price,
            receive_date_id,
            receive_datetime_id,
            active_flag,
            conclusion_price,
            conclusion_amount,
            yesterday_diff_price,
            yesterday_diff_rate,
            today_start_price,
            today_high_price,
            today_low_price,
            accumulate_trade_amount,
            yesterday_diff_sign,
            conclusion_flag,
            receive_datetime_id_per_20000,
            yesterday_calculate_price,
            yesterday_calculate_price_diff,
            yesterday_calculate_price_diff_price,
            yesterday_calculate_price_diff_rate,
            load_dttm
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, DATE_ADD(now(), INTERVAL 9 HOUR)
        )
    """
    for idx in range(0, len(pValue), 25):
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql, (pValue[idx:idx+25]))
        conn.commit()


def stockspurchase_overseas_futures(data_cnt, data):
    """해외선물옵션 체결처리 출력라이브러리"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 해외선물옵션 체결처리 출력라이브러리 !!!!!")
    # menulist = "종목코드|영업일자|장개시일자|장개시시각|장종료일자|장종료시각|전일종가|수신일자|수신시각|본장_전산장구분|체결가격|체결수량|전일대비가|등락률|시가|고가|저가|누적거래량|전일대비부호|체결구분|수신시각2만분의일초|전일정산가|전일정산가대비|전일정산가대비가격|전일정산가대비율"
    # menustr = menulist.split('|')
    pValue = data.split('^')
    i = 0
    for cnt in range(data_cnt):  # 넘겨받은 체결데이터 개수만큼 print 한다
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], "### [%d / %d]" % (cnt + 1, data_cnt))
        insert_to_aws_db_futures_option_realtime_info(pValue)
        # for menu in menustr:
        #     print ([time.strftime('%Y-%m-%d %H:%M:%S')], "%-13s[%s]" % (menu, pValue[i]))
        #     i += 1


async def get_overseas_futures_option_realtime_trade_price(item_dict):
    """해외선물옵션 실시간 체결가 조회"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 해외선물옵션 실시간 체결가 !!!!!")
    approval_key = get_approval()
    item_code_list = item_dict.keys()

    senddata_list = []
    for item_code in item_code_list:
        senddata = '{"header":{"approval_key": "%s","custtype":"P","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id":"HDFFF020","tr_key":"%s"}}}' % (approval_key, item_code)
        senddata_list.append(senddata)

    while True:
        async with websockets.connect(OPTION_URL_BASE, ping_interval=30) as websocket:
            for senddata in senddata_list:
                await websocket.send(senddata)
                time.sleep(0.5)
                print([time.strftime('%Y-%m-%d %H:%M:%S')], f"Input Command is :{senddata}")
            while True:
                try:
                    data = await websocket.recv()
                    time.sleep(0.5)
                    # 정제되지 않은 Request / Response 출력
                    print([time.strftime('%Y-%m-%d %H:%M:%S')], f"Recev Command is :{data}")
                    if data[0] == '0':
                        # 수신데이터가 실데이터 이전은 '|'로 나뉘어져있어 split
                        recvstr = data.split('|')
                        print([time.strftime('%Y-%m-%d %H:%M:%S')], "#### 해외선물옵션체결 ####")
                        data_cnt = int(recvstr[2])  # 체결데이터 개수
                        stockspurchase_overseas_futures(data_cnt, recvstr[3])
                except websockets.ConnectionClosed:
                    continue


####################################
##### 해외선물 옵션 대시보드 관련 함수 #####
####################################


def select_from_futures_option_realtime_info_in_Korean(days):
    """AWS DB furture_option_item_info.futures_option_realtime_info 테이블에서 한국 장중 값 SELECT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.futures_option_realtime_info 테이블에서 한국 장중 값 SELECT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    result_arr = []
    for day in range(0, days):
        sql = f"""
            SELECT st1.date_id
                 , st1.item_code
                 , st1.conclusion_price
                 , st2.conclusion_price
                 , st2.conclusion_price - st1.conclusion_price
                 , (st2.conclusion_price - st1.conclusion_price) / st1.conclusion_price * 100
                 , st2.accumulate_trade_amount - st1.accumulate_trade_amount
                 , st1.min_receive_dttm
                 , st2.max_receive_dttm
            FROM (
                  SELECT receive_date_id AS date_id
                       , item_code
                       , conclusion_price
                       , accumulate_trade_amount
                       , CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2)) AS min_receive_dttm
                    FROM furture_option_item_info.futures_option_realtime_info
                   WHERE seq IN (
                                 SELECT MIN(seq)
                                   FROM furture_option_item_info.futures_option_realtime_info
                                  WHERE receive_date_id = DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y%m%d')
                                    AND CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2))
                                        BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 09:00:00')
                                            AND CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 15:45:00')
                                  GROUP BY item_code
                                )
                 ) st1
           INNER JOIN (
                       SELECT receive_date_id AS date_id
                            , item_code
                            , conclusion_price
                            , accumulate_trade_amount
                            , CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2)) AS max_receive_dttm
                         FROM furture_option_item_info.futures_option_realtime_info
                        WHERE seq IN (
                                      SELECT MAX(seq)
                                        FROM furture_option_item_info.futures_option_realtime_info 
                                       WHERE receive_date_id = DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y%m%d')
                                         AND CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2))
                                             BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 09:00:00')
                                                 AND CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 15:45:00')
                                       GROUP BY item_code
                                     )
                      ) st2
                   ON (st1.date_id = st2.date_id
                        AND st1.item_code = st2.item_code
                      )
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        result_arr.append(result)
        conn.commit()
    return result_arr


def select_from_futures_option_realtime_info_out_Korean(days):
    """AWS DB furture_option_item_info.futures_option_realtime_info 테이블에서 한국 장외 값 SELECT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.futures_option_realtime_info 테이블에서 한국 장외 값 SELECT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    result_arr = []
    for day in range(0, days):
        pre_day = day + 1
        sql = f"""
            SELECT st1.date_id
                 , st1.item_code
                 , st1.conclusion_price
                 , IF(st3.conclusion_price, st3.conclusion_price, st2.conclusion_price)
                 , IF(st3.conclusion_price, st3.conclusion_price, st2.conclusion_price) - st1.conclusion_price
                 , (IF(st3.conclusion_price, st3.conclusion_price, st2.conclusion_price) - st1.conclusion_price) / st1.conclusion_price * 100
                 , IF(st3.accumulate_trade_amount, (st2.accumulate_trade_amount - st1.accumulate_trade_amount) + st3.accumulate_trade_amount, st1.accumulate_trade_amount + st2.accumulate_trade_amount)
                 , st1.min_receive_dttm
                 , IF(st3.max_receive_dttm, st3.max_receive_dttm, st2.max_receive_dttm)
            FROM (
                  SELECT DATE_FORMAT(DATE_ADD(receive_date_id, INTERVAL 1 day), '%Y%m%d') AS date_id
                       , item_code
                       , conclusion_price
                       , accumulate_trade_amount
                       , CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2)) AS min_receive_dttm
                    FROM furture_option_item_info.futures_option_realtime_info
                   WHERE seq IN (
                                 SELECT MIN(seq)
                                   FROM furture_option_item_info.futures_option_realtime_info
                                  WHERE receive_date_id = DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {pre_day} day), '%Y%m%d')
                                    AND CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2))
                                        BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {pre_day} day), '%Y-%m-%d'), ' 15:45:00')
                                            AND CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 06:00:00')
                                  GROUP BY item_code
                                )
                 ) st1
           INNER JOIN (
                       SELECT receive_date_id AS date_id
                            , item_code
                            , conclusion_price
                            , accumulate_trade_amount
                            , CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2)) AS max_receive_dttm
                         FROM furture_option_item_info.futures_option_realtime_info
                        WHERE seq IN (
                                      SELECT MAX(seq)
                                        FROM furture_option_item_info.futures_option_realtime_info 
                                       WHERE receive_date_id = DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y%m%d')
                                         AND CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2))
                                             BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {pre_day} day), '%Y-%m-%d'), ' 15:45:00')
                                                 AND CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 06:00:00')
                                       GROUP BY item_code
                                     )
                      ) st2
                   ON (st1.date_id = st2.date_id
                       AND st1.item_code = st2.item_code
                      )
            LEFT OUTER JOIN (
                             SELECT receive_date_id AS date_id
                                  , item_code
                                  , conclusion_price
                                  , accumulate_trade_amount
                                  , CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2)) AS max_receive_dttm
                              FROM furture_option_item_info.futures_option_realtime_info
                             WHERE seq IN (
                                           SELECT MAX(seq)
                                             FROM furture_option_item_info.futures_option_realtime_info 
                                            WHERE receive_date_id = DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y%m%d')
                                              AND CONCAT(DATE_FORMAT(receive_date_id, '%Y-%m-%d'), ' ', LEFT(receive_datetime_id, 2), ':', SUBSTRING(receive_datetime_id, 3, 2), ':', RIGHT(receive_datetime_id, 2))
                                                  BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 07:00:00')
                                                      AND CONCAT(DATE_FORMAT(DATE_SUB(DATE_ADD(NOW(), INTERVAL 9 HOUR), INTERVAL {day} day), '%Y-%m-%d'), ' 09:00:00')
                                            GROUP BY item_code
                                          )
                            ) st3
                         ON (st1.date_id = st3.date_id
                             AND st1.item_code = st3.item_code
                            )
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        result_arr.append(result)
        conn.commit()
    return result_arr


def insert_to_oversea_futures_option_dashboard(values, is_in_Korean):
    """AWS DB furture_option_item_info.oversea_futures_option_dashboard 테이블에 INSERT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.oversea_futures_option_dashboard 테이블에 INSERT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    result_arr = []
    for val in values:
        sql = f"""
        DELETE FROM furture_option_item_info.oversea_futures_option_dashboard
              WHERE date_id = '{val[0]}'
                AND item_code = '{val[1]}'
                AND is_in_Korean = '{is_in_Korean}'
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        conn.commit()
        sql = f"""
        INSERT INTO furture_option_item_info.oversea_futures_option_dashboard (
            date_id,
            item_code,
            is_in_Korean,
            first_price,
            last_price,
            diff_price,
            diff_price_rate,
            conclusion_amount,
            min_receive_dttm,
            max_receive_dttm,
            load_dttm
        )
        VALUES (
            %s, %s, '{is_in_Korean}',
            %s, %s, %s,
            %s, %s, %s, %s, DATE_ADD(now(), INTERVAL 9 HOUR)
        )
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql, val)
        conn.commit()
    return result_arr


##################################
##### 옵션 대시보드 결과 관련 함수 #####
##################################


def copy_to_yesterday_today_item_result(values, is_in_Korean):
    """AWS DB furture_option_item_info.yesterday_today_item_result 테이블에 INSERT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.yesterday_today_item_result 테이블에 INSERT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    for val in values:
        sql = f"""
        DELETE FROM furture_option_item_info.yesterday_today_item_result
              WHERE date_id = '{val[0]}'
                AND oversea_item_code = '{val[1]}'
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        conn.commit()
        sql = f"""
        INSERT INTO furture_option_item_info.yesterday_today_item_result (
            date_id,
            oversea_item_code,
            oversea_yesterday_today_diff_price_rate,
            oversea_yesterday_today_diff_conclusion_amount,
            load_dttm
        )
        SELECT date_id
             , item_code
             , diff_price_rate
             , conclusion_amount
             , DATE_ADD(now(), INTERVAL 9 HOUR)
          FROM furture_option_item_info.oversea_futures_option_dashboard
         WHERE date_id = '{val[0]}'
           AND item_code = '{val[1]}'
           AND is_in_Korean = '{is_in_Korean}'
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        conn.commit()


def update_to_yesterday_today_item_result(values, is_in_Korean):
    """AWS DB furture_option_item_info.yesterday_today_item_result 테이블에 UPDATE"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.yesterday_today_item_result 테이블에 UPDATE !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    for val in values:
        sql = f"""
        UPDATE furture_option_item_info.yesterday_today_item_result st1
         INNER JOIN (
                     SELECT date_id
                          , item_code
                          , diff_price_rate
                          , conclusion_amount
                       FROM furture_option_item_info.oversea_futures_option_dashboard
                      WHERE date_id = '{val[0]}'
                        AND item_code = '{val[1]}'
                        AND is_in_Korean = '{is_in_Korean}'
                    ) st2
                 ON (IF(DAYOFWEEK(st1.date_id) = 7, DATE_FORMAT(DATE_ADD(st1.date_id, INTERVAL 2 DAY), '%Y%m%d'), st1.date_id) = st2.date_id
                     AND st1.oversea_item_code = st2.item_code
                    )
           SET oversea_today_diff_price_rate = st2.diff_price_rate
             , oversea_today_diff_conclusion_amount = st2.conclusion_amount
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        conn.commit()
        sql = f"""
        UPDATE furture_option_item_info.yesterday_today_item_result st1
         INNER JOIN (
                     SELECT date_id
                          , item_code
                          , (futs_prpr - futs_oprc) / futs_oprc * 100 AS Korean_today_diff_price_rate
                          , acml_vol AS Korean_today_diff_conclusion_amount
                          , total_askp_rsqn AS Korean_total_wanna_sell_balance
                          , total_bidp_rsqn AS Korean_total_wanna_buy_balance
                       FROM furture_option_item_info.futures_option_Korean_realtime_info
                      WHERE date_id = '{val[0]}'
                      ORDER BY seq DESC
                      LIMIT 1
                    ) st2
                 ON (IF(DAYOFWEEK(st1.date_id) = 7, DATE_FORMAT(DATE_ADD(st1.date_id, INTERVAL 2 DAY), '%Y%m%d'), st1.date_id) = st2.date_id
                    )
           SET st1.Korean_item_code = st2.item_code
             , st1.Korean_today_diff_price_rate = st2.Korean_today_diff_price_rate
             , st1.Korean_today_diff_conclusion_amount = st2.Korean_today_diff_conclusion_amount
             , st1.Korean_total_wanna_sell_balance = st2.Korean_total_wanna_sell_balance
             , st1.Korean_total_wanna_buy_balance = st2.Korean_total_wanna_buy_balance
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        conn.commit()
        sql = f"""
        UPDATE furture_option_item_info.yesterday_today_item_result
           SET oversea_Korean_yesterday_diff_price_rate = oversea_yesterday_today_diff_price_rate - Korean_today_diff_price_rate
             , oversea_Korean_today_diff_price_rate = oversea_today_diff_price_rate - Korean_today_diff_price_rate
         WHERE date_id = IF(DAYOFWEEK('{val[0]}') = 2, DATE_FORMAT(DATE_SUB('{val[0]}', INTERVAL 2 DAY), '%Y%m%d'), '{val[0]}')
        """
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql)
        conn.commit()


#########################################
##### 해외주식 실시간체결통보 옵션 관련 함수 #####
#########################################

async def get_Overseas_stock_realtime_trade_price(item_code):
    """해외주식 실시간체결통보"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 해외주식 실시간체결통보 !!!!!")
    approval_key = get_approval()

    senddata_list = []
    senddata = '{"header":{"approval_key": "%s","custtype":"P","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id":"H0GSCNI0","tr_key":"%s"}}}' % (approval_key, item_code)
    senddata_list.append(senddata)

    while True:
        async with websockets.connect(OPTION_URL_BASE, ping_interval=30) as websocket:
            for senddata in senddata_list:
                await websocket.send(senddata)
                time.sleep(0.5)
                print([time.strftime('%Y-%m-%d %H:%M:%S')], f"Input Command is :{senddata}")
            while True:
                try:
                    data = await websocket.recv()
                    time.sleep(0.5)
                    # 정제되지 않은 Request / Response 출력
                    print([time.strftime('%Y-%m-%d %H:%M:%S')], f"Recev Command is :{data}")
                    if data[0] == '0':
                        # 수신데이터가 실데이터 이전은 '|'로 나뉘어져있어 split
                        recvstr = data.split('|')
                        print([time.strftime('%Y-%m-%d %H:%M:%S')], "#### 해외주식 체결 ####")
                        data_cnt = int(recvstr[2])  # 체결데이터 개수
                        stockspurchase_Overseas_stock(data_cnt, recvstr[3])
                except websockets.ConnectionClosed:
                    continue


def stockspurchase_Overseas_stock(data_cnt, data):
    """해외주식 체결처리 출력라이브러리"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 해외주식 체결처리 출력라이브러리 !!!!!")
    # menulist = "선물단축종목코드|영업시간|선물전일대비|전일대비부호|선물전일대비율|선물현재가|선물시가2|선물최고가|선물최저가|최종거래량|누적거래량|누적거래대금|HTS이론가|>시장베이시스|괴리율|근월물약정가|원월물약정가|스프레드1|HTS미결제약정수량|미결제약정수량증감|시가시간|시가2대비현재가부호|시가대비지수현재가|최고가시간|최고가대비현재가부호|최고가대비지수현재가|최저가시간|최저가대비현재가부호|최저가대비지수현재가|매수2비율|체결강도|괴리도|미결제약정직전수량증감|이론베이시스|선>물매도호가1|선물매수호가1|매도호가잔량1|매수호가잔량1|매도체결건수|매수체결건수|순매수체결건수|총매도수량|총매수수량|총매도호가잔량|총매수호가잔량|전일거래>량대비등락율|협의대량거래량|실시간상한가|실시간하한가|실시간가격제한구분"
    # menustr = menulist.split('|')
    pValue = data.split('^')
    # print ([time.strftime('%Y-%m-%d %H:%M:%S')], "chk pValue!! : ", pValue)
    i = 0
    for cnt in range(data_cnt):  # 넘겨받은 체결데이터 개수만큼 print 한다
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], "### [%d / %d]" % (cnt + 1, data_cnt))
        insert_to_aws_db_tb_overseas_stock_realtime_info(pValue)
        # for menu in menustr:
        #     print ([time.strftime('%Y-%m-%d %H:%M:%S')], "%-13s[%s]" % (menu, pValue[i]))
        #     i += 1


def insert_to_aws_db_tb_overseas_stock_realtime_info(pValue):
    """AWS DB furture_option_item_info.tb_overseas_stock_realtime_info 테이블에 INSERT"""
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : AWS DB furture_option_item_info.tb_overseas_stock_realtime_info 테이블에 INSERT !!!!!")
    conn = connect_to_aws_db()
    cursor = conn.cursor()
    sql = """
        INSERT INTO furture_option_item_info.tb_overseas_stock_realtime_info (
            CUST_ID
            , ACNT_NO
            , ODER_NO
            , OODER_NO
            , SELN_BYOV_CLS
            , RCTF_CLS
            , ODER_KIND2
            , STCK_SHRN_ISCD
            , CNTG_QTY
            , CNTG_UNPR
            , STCK_CNTG_HOUR
            , RFUS_YN
            , CNTG_YN
            , ACPT_YN
            , BRNC_NO
            , ODER_QTY
            , ACNT_NAME
            , CNTG_ISNM
            , ODER_COND
            , DEBT_GB
            , DEBT_DATE
            , load_dttm
        )
        VALUES (
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 9 HOUR), '%%Y%%m%%d'),
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            DATE_ADD(now(), INTERVAL 9 HOUR)
        )
    """
    for idx in range(0, len(pValue), 21):
        # print ([time.strftime('%Y-%m-%d %H:%M:%S')], sql)
        cursor.execute(sql, (pValue[idx:idx+21]))
        conn.commit()
