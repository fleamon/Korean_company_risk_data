import CommonFunction as cf
import pandas as pd
from datetime import datetime, timedelta
from transformers import ReformerTokenizer, ReformerModel
import torch


tokenizer = ReformerTokenizer.from_pretrained('google/reformer-crime-and-punishment')
model = ReformerModel.from_pretrained('google/reformer-crime-and-punishment')


def calculate_bert_score(text):
    if not text.strip():
        return 0.0
    # 텍스트를 토큰으로 분리
    tokens = tokenizer.encode(text, add_special_tokens=True, return_tensors='pt')
    # Reformer 모델을 통해 embedding 벡터 계산
    output = model(tokens)[0]
    # 평균 풀링을 통해 문장 임베딩 벡터 계산
    sentence_embedding = torch.mean(output, dim=1)
    # 문장 임베딩 벡터의 L2 노름 계산 (스코어로 사용)
    score = torch.norm(sentence_embedding, dim=1)
    
    return score.item()


def main(company_ceo_name, start_date, end_date):
    start_date = start_date.replace('.', '-')
    end_date = end_date.replace('.', '-')

    conn = cf.connect_to_db()
    cursor = conn.cursor()
               
    # MySQL 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_Korean_by_ESG_BackData.articles_bert_scoring (
    seq bigint NOT NULL AUTO_INCREMENT,
    date_id date DEFAULT NULL COMMENT '날짜',
    company_name varchar(255) DEFAULT NULL COMMENT '기업명',
    title_index double DEFAULT NULL COMMENT '제목Score',
    article_index double DEFAULT NULL COMMENT '기사Score',
    load_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '저장시간',
    PRIMARY KEY (seq),
    KEY idx_date_id (date_id),
    KEY idx_company_name (company_name)
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
    
        query = f"SELECT article_reg_date, company_name, title, article_text FROM stock_Korean_by_ESG_BackData.articles WHERE company_name = '{target_firm}' AND article_reg_date = '{target_date}'"
        df = pd.read_sql(query, conn)
        
        for date, group in df.groupby('article_reg_date'):
            title_scores = []
            text_scores = []
            for index, row in group.iterrows():
                title_score = calculate_bert_score(row['title'])
                text_score = calculate_bert_score(row['article_text'])
                title_scores.append(title_score)
                text_scores.append(text_score)
            
            avg_title_score = sum(title_scores) / len(title_scores)
            avg_text_score = sum(text_scores) / len(text_scores)
            # print(f"Average Title Score: {avg_title_score}, Average Text Score: {avg_text_score}")

            delete_query = f'''
                DELETE FROM stock_Korean_by_ESG_BackData.articles_bert_scoring
                WHERE date_id = '{target_date}'
                AND company_name = '{target_firm}'
            '''
            cursor.execute(delete_query)
            conn.commit()

            insert_query = f'''
                INSERT INTO stock_Korean_by_ESG_BackData.articles_bert_scoring
                (date_id, company_name, title_index, article_index, load_date)
                VALUES
                ('{target_date}', '{target_firm}', {avg_title_score}, {avg_text_score}, NOW())
                ON DUPLICATE KEY UPDATE 
                date_id=VALUES(date_id), 
                company_name=VALUES(company_name)
            '''
            # print (insert_query)
            # break  # for debug
            cursor.execute(insert_query)
            conn.commit()

    cursor.close()
    conn.close()

    cf.send_message("KOR", "Bert scoring success")
    print(" Bert 스코어링 데이터 저장 successfully.")