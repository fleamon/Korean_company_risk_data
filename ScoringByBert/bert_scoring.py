import dill
import CommonFunction as cf
import pandas as pd


def main():
    conn = cf.connect_to_db()
    cursor = conn.cursor()
    truncate_query = """TRUNCATE TABLE stock_Korean_by_ESG_BackData.articles_bert_scoring"""
    cursor.execute(truncate_query)
    conn.commit()
            
    # dill 파일에서 데이터 읽기
    with open('./dill_files/20240215_0038_score_dataframes.dill', 'rb') as f:
        data = dill.load(f)

    # print (data)  # type : <class 'pandas.core.frame.DataFrame'>
    data_column_names_value = ', '.join(data.columns[1:])
    # print(data_column_names_value)  #type : <class 'str'>
    
    daily_com_grouped_df  = data[[
    'article_reg_date', 'company_name',
    'title_positive_score', 'title_negative_score', 'article_positive_score', 'article_negative_score'
    ]].groupby( ['article_reg_date', 'company_name']).sum()
    # print (daily_com_grouped_df)  # type : <class 'pandas.core.frame.DataFrame'>
    daily_com_grouped_df_columns = ', '.join(daily_com_grouped_df.columns)
    # print(daily_com_grouped_df_columns)  #type : <class 'str'>

    scores_df = pd.DataFrame()
    scores_df['title_index'] = daily_com_grouped_df['title_positive_score'] -  daily_com_grouped_df['title_negative_score'] 
    scores_df['article_index'] = daily_com_grouped_df['article_positive_score'] -  daily_com_grouped_df['article_negative_score']
    print (scores_df)  # type : <class 'pandas.core.frame.DataFrame'>
    scores_df_columns = ', '.join(scores_df.columns)
    print (scores_df_columns)  #type : <class 'str'>

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

    # 데이터베이스에 데이터 삽입
    for index, row in scores_df.iterrows():
        # print (row.name)  # (datetime.date(2020, 12, 7), '삼성전자')
        # print (row)  # type : <class 'pandas.core.series.Series'>
        insert_query = f'''
            INSERT INTO stock_Korean_by_ESG_BackData.articles_bert_scoring
            (date_id, company_name, {scores_df_columns}, load_date)
            VALUES
            ('{row.name[0]}', '{row.name[1]}'
            , {row["title_index"]}, {row["article_index"]}, NOW())
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

    cf.send_message("KOR", "Bert scoring success by score_dataframes.dill")
    print(" Bert 스코어링 데이터 저장 successfully.")