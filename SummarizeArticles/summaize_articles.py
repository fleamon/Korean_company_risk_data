import pandas as pd
import CommonFunction as cf
import requests
import json
from datetime import datetime, timedelta


# [내 애플리케이션] > [앱 키] 에서 확인한 REST API 키 값 입력
REST_API_KEY = '477fc15718fc798560927cde08df28c8'

# KoGPT API 호출을 위한 함수
# 각 파라미터 기본값으로 설정
def kogpt_api(prompt, max_tokens = 1, temperature = 1.0, top_p = 0, n = 1):
    r = requests.post(
        'https://api.kakaobrain.com/v1/inference/kogpt/generation',
        json = {
            'prompt': prompt,
            'max_tokens': max_tokens,
            'temperature': temperature,
            'top_p': top_p,
            'n': n
        },
        headers = {
            'Authorization': 'KakaoAK ' + REST_API_KEY,
            'Content-Type': 'application/json'
        }
    )
    # 응답 JSON 형식으로 변환
    response = json.loads(r.content)
    return response


def main(company_ceo_name, start_date, end_date):
    start_date = start_date.replace('.', '-')
    end_date = end_date.replace('.', '-')

    conn = cf.connect_to_db()
    cursor = conn.cursor()
    query = f"SELECT * FROM stock_Korean_by_ESG_BackData.articles WHERE article_reg_date BETWEEN '{end_date}' AND '{start_date}'"
    data = pd.read_sql(query, conn)
    
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
        # KoGPT에게 전달할 명령어 구성
        article_texts = ' '.join(data.query( "company_name == @target_firm and article_reg_date == @target_date ")['article_text'].replace(r'\n+', ' ', regex=True) )
        #prompt = article_texts + " \n 한줄 요약 : "
        prompt =  article_texts[:2000] + " \n 한줄 요약 : "

        # 파라미터를 전달해 kogpt_api()메서드 호출
        response = kogpt_api(
            prompt,
            max_tokens = 1028,
            temperature = 0.1,  # 최대한 덜 창의
            top_p = 0.1,  # 최대한 덜 창의
            #n = 1  # 1~16
        )

        # print (response['generations'][0]['text'])  # 한줄요약 확인
        articles_summary = cf.delete_patterns(response['generations'][0]['text'])

        # MySQL 테이블 생성
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_Korean_by_ESG_BackData.summaize_articles (
        seq bigint NOT NULL AUTO_INCREMENT,
        company_name varchar(255) DEFAULT NULL COMMENT '기업명',
        article_reg_date date DEFAULT NULL COMMENT '기사 발행일',
        articles_summary text DEFAULT NULL COMMENT '기사 한줄요약',
        load_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '저장시간',
        PRIMARY KEY (seq),
        KEY idx_company_name (company_name),
        KEY idx_article_reg_date (article_reg_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
        """
        cursor.execute(create_table_query)
        conn.commit()

        # delete
        delete_query = f'''
        DELETE FROM stock_Korean_by_ESG_BackData.summaize_articles
            WHERE company_name = '{target_firm}'
            AND article_reg_date = '{target_date}'
        '''
        cursor.execute(delete_query)

        # insert
        insert_query = f'''
            INSERT INTO stock_Korean_by_ESG_BackData.summaize_articles
            (company_name, article_reg_date, articles_summary, load_date)
            VALUES
            ('{target_firm}', '{target_date}', '{articles_summary}', NOW())
        '''
        cursor.execute(insert_query)
        conn.commit()

    cursor.close()
    conn.close()

    cf.send_message("KOR", "articles summarized success")
    print("해당일 해당기업 모든기사 한줄요약 데이터 저장 successfully.")