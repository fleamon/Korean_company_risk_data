import CommonFunction as cf
import dart_fss as dart_fss
import pandas as pd


def main():
    dart_fss.set_api_key(api_key=cf.DART_API_KEY)
    # print (dart_fss)

    all_corp = dart_fss.api.filings.get_corp_code()
    # print (len(all_corp))
    # print (all_corp)

    all_corp_df = pd.DataFrame(all_corp)

    # 상장
    df_listed = all_corp_df[all_corp_df['stock_code'].notnull()]
    print (len(df_listed))
    print (df_listed)
    df_listed_columns = ','.join(df_listed.columns.to_list())
    print (df_listed_columns)

    # 비상장
    df_non_listed = all_corp_df[all_corp_df['stock_code'].isnull()]
    # print (len(df_non_listed))
    # print (df_non_listed)

    # corp_code = df_listed[df_listed['corp_name'] == '카카오']
    # print (corp_code)
    # print (dart_fss.api.filings.get_corp_info(corp_code))
    # print (type(dart_fss.api.filings.get_corp_info(corp_code)))

    conn = cf.connect_to_db()
    cursor = conn.cursor()
           
    # MySQL 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Korean_company_risk_data_meta.company_list (
    seq bigint NOT NULL AUTO_INCREMENT,
    corp_code varchar(50) DEFAULT NULL COMMENT '기업명',
    corp_name varchar(50) DEFAULT NULL COMMENT '제목Score',
    stock_code varchar(50) DEFAULT NULL COMMENT '기사Score',
    modify_date varchar(50) DEFAULT NULL COMMENT '기사Score',
    load_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '저장시간',
    PRIMARY KEY (seq),
    KEY idx_corp_name (corp_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """
    cursor.execute(create_table_query)
    conn.commit()

    # MySQL 테이블 생성
    truncate_table_query = """
    TRUNCATE TABLE Korean_company_risk_data_meta.company_list
    """
    cursor.execute(truncate_table_query)
    conn.commit()

    # 데이터베이스에 데이터 삽입
    for index, row in df_listed.iterrows():
        # print (row.name)
        # print (row)
        insert_query = f'''
            INSERT INTO Korean_company_risk_data_meta.company_list
            ({df_listed_columns}, load_date)
            VALUES
            ('{row["corp_code"]}', '{row["corp_name"]}',
            '{row["stock_code"]}','{row["modify_date"]}', NOW())
            ON DUPLICATE KEY UPDATE 
            corp_name=VALUES(corp_name)
        '''
        # print (insert_query)
        # break  # for debug
        cursor.execute(insert_query)
        conn.commit()

    cursor.close()
    conn.close()

    cf.send_message("KOR", "Dart corp list success")
    print("Dart 회사목록 저장 successfully.")