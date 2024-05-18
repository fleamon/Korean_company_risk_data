import CommonFunction as cf
import dart_fss as dart_fss
import pandas as pd


def main(company_ceo_name, start_date, end_date):
    if '+' in company_ceo_name:
        company_name = company_ceo_name.split('+')[0]
        ceo_name = company_ceo_name.split('+')[1]
    else:
        company_name = company_ceo_name
    dart_fss.set_api_key(api_key=cf.DART_API_KEY)
    # print (dart_fss)

    corp_list = dart_fss.corp.get_corp_list()
    # print (corp_list)
    # print (corp_list.corps)
    # print ()
    company_list = corp_list.find_by_corp_name(company_name, exactly=True)

    print (type(company_list))
    for company_info in company_list:
        print (company_info)
        print (type(company_info))
        corp_number = company_info[0]
        corp_name = company_info[1]
        corp_info_dict = dart_fss.api.filings.get_corp_info(corp_number)
        print (corp_info_dict)
        print (type(corp_info_dict))
    """
    # corp_list = dart_fss.get_corp_list()
    # # print (corp_list.corps)

    all = dart_fss.api.filings.get_corp_code()
    # print (all)

    df = pd.DataFrame(all)
    # print (df)

    # 상장
    df_listed = df[df['stock_code'].notnull()]
    print (df_listed)

    # # 비상장
    # df_non_listed = df[df['stock_code'].isnull()]
    # print (df_non_listed)

    # # df_listed.to_excel('public_item.xlsx')
    # # df_non_listed.to_excel('not_public_item.xlsx')


    corp_code = df_listed[df_listed['corp_name'] == '카카오'].iloc[0,0]
    print (dart_fss.api.filings.get_corp_info(corp_code))
    print (type(dart_fss.api.filings.get_corp_info(corp_code)))
    """

    # conn = cf.connect_to_db()
    # cursor = conn.cursor()
           
    # # MySQL 테이블 생성
    # create_table_query = """
    # CREATE TABLE IF NOT EXISTS stock_Korean_by_ESG_BackData.articles_bert_scoring (
    # seq bigint NOT NULL AUTO_INCREMENT,
    # date_id date DEFAULT NULL COMMENT '날짜',
    # company_name varchar(255) DEFAULT NULL COMMENT '기업명',
    # title_index double DEFAULT NULL COMMENT '제목Score',
    # article_index double DEFAULT NULL COMMENT '기사Score',
    # load_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '저장시간',
    # PRIMARY KEY (seq),
    # KEY idx_date_id (date_id),
    # KEY idx_company_name (company_name)
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    # """
    # cursor.execute(create_table_query)
    # conn.commit()

    # # 데이터베이스에 데이터 삽입
    # for index, row in scores_df.iterrows():
    #     # print (row.name)  # (datetime.date(2020, 12, 7), '삼성전자')
    #     # print (row)  # type : <class 'pandas.core.series.Series'>
    #     delete_query = f'''
    #         DELETE FROM stock_Korean_by_ESG_BackData.articles_bert_scoring
    #         WHERE date_id = '{row.name[0]}'
    #           AND company_name = '{row.name[1]}'
    #     '''
    #     cursor.execute(delete_query)
    #     conn.commit()

    #     insert_query = f'''
    #         INSERT INTO stock_Korean_by_ESG_BackData.articles_bert_scoring
    #         (date_id, company_name, {scores_df_columns}, load_date)
    #         VALUES
    #         ('{row.name[0]}', '{row.name[1]}'
    #         , {row["title_index"]}, {row["article_index"]}, NOW())
    #         ON DUPLICATE KEY UPDATE 
    #         date_id=VALUES(date_id), 
    #         company_name=VALUES(company_name)
    #     '''
    #     # print (insert_query)
    #     # break  # for debug
    #     cursor.execute(insert_query)
    #     conn.commit()

    # cursor.close()
    # conn.close()

    # cf.send_message("KOR", "Dart information success")
    # print("Dart 정보 데이터 저장 successfully.")