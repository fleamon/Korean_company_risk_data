import pandas as pd
import CommonFunction as cf
from datetime import datetime, timedelta
import os
import gspread
from google.oauth2.service_account import Credentials


def main(company_ceo_name, start_date, end_date):
    current_path = os.getcwd()
    # 서비스 계정 키 파일 경로
    SERVICE_ACCOUNT_FILE = f'{current_path}/env/fluted-union-425618-q4-a7a2d23ac8d2.json'

    # 접근 범위 정의
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    # 자격 증명 설정
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # gspread 클라이언트 생성
    client = gspread.authorize(creds)

    # Google Sheets 문서의 ID와 시트 이름
    SPREADSHEET_ID = '1SEs_cwQfCK3j6Ltad5jHyTiH8xf9xLvR_PzahGVhNIo'
    SHEET_NAME = 'ESG NP Keyword'

    # 스프레드시트
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    sheet = spreadsheet.worksheet(SHEET_NAME)
    
    data = sheet.get_values()
    
    # keyword, weight
    e_posi_keyword = []
    e_posi_weight = []
    e_nega_keyword = []
    e_nega_weight = []
    s_posi_keyword = []
    s_posi_weight = []
    s_nega_keyword = []
    s_nega_weight = []
    g_posi_keyword = []
    g_posi_weight = []
    g_nega_keyword = []
    g_nega_weight = []
    for row in data[2:]:
        e_posi_keyword.append(row[0])
        e_posi_weight.append(row[1])
        e_nega_keyword.append(row[2])
        e_nega_weight.append(row[3])
        s_posi_keyword.append(row[4])
        s_posi_weight.append(row[5])
        s_nega_keyword.append(row[6])
        s_nega_weight.append(row[7])
        g_posi_keyword.append(row[8])
        g_posi_weight.append(row[9])
        g_nega_keyword.append(row[10])
        g_nega_weight.append(row[11])

    e_posi_keyword = [element for element in e_posi_keyword if element]
    e_posi_weight = [int(element) * -1 for element in e_posi_weight if element]
    e_nega_keyword = [element for element in e_nega_keyword if element]
    e_nega_weight = [element for element in e_nega_weight if element]
    s_posi_keyword = [element for element in s_posi_keyword if element]
    s_posi_weight = [int(element) * -1 for element in s_posi_weight if element]
    s_nega_keyword = [element for element in s_nega_keyword if element]
    s_nega_weight = [element for element in s_nega_weight if element]
    g_posi_keyword = [element for element in g_posi_keyword if element]
    g_posi_weight = [int(element) * -1 for element in g_posi_weight if element]
    g_nega_keyword = [element for element in g_nega_keyword if element]
    g_nega_weight = [element for element in g_nega_weight if element]

    start_date = start_date.replace('.', '-')
    end_date = end_date.replace('.', '-')

    conn = cf.connect_to_db()
    cursor = conn.cursor()
    
    # MySQL 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_Korean_by_ESG_BackData.articles_posi_nega_scoring (
    seq bigint NOT NULL AUTO_INCREMENT,
    date_id date DEFAULT NULL COMMENT '날짜',
    company_name varchar(255) DEFAULT NULL COMMENT '기업명',
    positive_score double DEFAULT NULL COMMENT '긍정Score',
    negative_score double DEFAULT NULL COMMENT '부정Score',
    load_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '저장시간',
    PRIMARY KEY (seq),
    KEY idx_date_id (date_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """
    cursor.execute(create_table_query)
    conn.commit()

    # 기업, 날짜
    if '+' in company_ceo_name:
        target_firm = company_ceo_name.split('+')[0]
    else:
        target_firm = company_ceo_name
    
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    # 시작 날짜부터 종료 날짜까지 하루씩 감소
    current_datetime = start_datetime
    while current_datetime >= end_datetime:
        target_date = str(current_datetime.strftime("%Y-%m-%d"))
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
        current_datetime -= timedelta(days=1)
        print ("company_name :", target_firm)
        print ("article_reg_date :", target_date)

        query = f"SELECT article_reg_date, company_name, title, article_text FROM stock_Korean_by_ESG_BackData.articles WHERE article_reg_date = '{target_date}' AND company_name = '{target_firm}'"
        data = pd.read_sql(query, conn)
        if data.empty:
            continue
        
        posi_keyword_list = e_posi_keyword + s_posi_keyword + g_posi_keyword
        posi_weight_list = e_posi_weight + s_posi_weight + g_posi_weight
        posi_score = 0
        for keyword, weight in zip(posi_keyword_list, posi_weight_list):
            posi_score += data['article_text'].str.count(keyword) * int(weight)

        nega_score = 0
        nega_keyword_list = e_nega_keyword + s_nega_keyword + g_nega_keyword
        nega_weight_list = e_nega_weight + s_nega_weight + g_nega_weight
        for keyword, weight in zip(nega_keyword_list, nega_weight_list):
            nega_score += data['article_text'].str.count(keyword) * int(weight)
        
        # 데이터베이스에 데이터 삽입
        delete_query = f'''
            DELETE FROM stock_Korean_by_ESG_BackData.articles_posi_nega_scoring
            WHERE date_id = '{target_date}'
            AND company_name = '{target_firm}'
        '''
        # print (delete_query)
        cursor.execute(delete_query)
        conn.commit()

        insert_query = f'''
            INSERT INTO stock_Korean_by_ESG_BackData.articles_posi_nega_scoring
            (date_id, company_name, positive_score, negative_score, load_date)
            VALUES
            ('{target_date}'
            , '{target_firm}'
            , {posi_score.sum()}
            , {nega_score.sum()}
            , NOW())
            ON DUPLICATE KEY UPDATE 
            date_id=VALUES(date_id)
        '''
        # print (insert_query)
        cursor.execute(insert_query)
        conn.commit()

    cursor.close()
    conn.close()

    cf.send_message("KOR", "posi / nega scoring success")
    print("긍정 / 부정 스코어링 데이터 저장 successfully.")