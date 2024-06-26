import dill
import pandas as pd
import CommonFunction as cf
from datetime import datetime, timedelta
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances, manhattan_distances


def main(company_ceo_name, start_date, end_date):
    with open('./dill_files/score_dataframes.dill', 'rb') as f:
        data = dill.load(f)

    print ("tfidf_vectorizer")
    tfidf_vectorizer = TfidfVectorizer()
    print ("tfidf_matrix")
    tfidf_matrix = tfidf_vectorizer.fit_transform(data['article_text'])
    
    print ("cosine_similarities")
    cosine_similarities = cosine_similarity(tfidf_matrix, tfidf_matrix)
    print ("euclidean_distances_matrix")
    euclidean_distances_matrix = euclidean_distances(tfidf_matrix, tfidf_matrix)
    print ("manhattan_distances_matrix")
    manhattan_distances_matrix = manhattan_distances(tfidf_matrix, tfidf_matrix)
    
    cosine_sim_df = pd.DataFrame(cosine_similarities,  
                                columns = data['seq'] , 
                                index = pd.MultiIndex.from_frame( data[['seq', 'article_reg_date', 'company_name',]] ) , 
                                )
    
    # rank로 변환
    cosine_sim_rank_df = cosine_sim_df.rank(axis=1, method='min')
    euclidean_dist_df = pd.DataFrame(euclidean_distances_matrix,  
                                columns = data['seq'] , 
                                index = pd.MultiIndex.from_frame( data[['seq', 'article_reg_date', 'company_name',]] ) , 
                                )
    # 거리 짧을수록 높은 점수
    euclidean_dist_rank_df = euclidean_dist_df.rank(axis=1, ascending = False)
    manhattan_dist_df = pd.DataFrame(manhattan_distances_matrix,  
                                columns = data['seq'] , 
                                index = pd.MultiIndex.from_frame( data[['seq', 'article_reg_date', 'company_name',]] ) , 
                                )
    #거리 짧을수록 높은 점수
    manhattan_dist_rank_df = manhattan_dist_df.rank(axis=1, ascending = False)
    
    conn = cf.connect_to_db()
    cursor = conn.cursor()

    # MySQL 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_Korean_by_ESG_BackData.compared_articles (
    seq bigint NOT NULL AUTO_INCREMENT,
    articles_id bigint DEFAULT NULL COMMENT 'articles 테이블 seq',
    company_name varchar(255) DEFAULT NULL COMMENT '기업명',
    article_reg_date date DEFAULT NULL COMMENT '기사 발행일',
    title varchar(255) DEFAULT NULL COMMENT '제목',
    article_text longtext DEFAULT NULL COMMENT '기사',
    load_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '저장시간',
    PRIMARY KEY (seq),
    KEY idx_article_reg_date (article_reg_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # 해당 기업, 날짜
    if '+' in company_ceo_name:
        impact_firm = company_ceo_name.split('+')[0]
    else:
        impact_firm = company_ceo_name
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    # 시작 날짜부터 종료 날짜까지 하루씩 감소
    current_datetime = start_datetime
    while current_datetime >= end_datetime:
        impact_date = str(current_datetime.strftime("%Y-%m-%d"))
        current_datetime -= timedelta(days=1)
        """
        # 코사인
        # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
        #추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
        seq_series = cosine_sim_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
        rank_sorted_series = cosine_sim_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1).sort_values(ascending=False,)
        
        # 상위 기사 선택 후 보여주기
        how_rank_len = 10
        sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head(how_rank_len)
        cosine_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
        
        # 유클리드
        # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
        #추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
        seq_series = euclidean_dist_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
        rank_sorted_series = euclidean_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1).sort_values(ascending=False,)
        
        # 상위 기사 선택 후 보여주기
        how_rank_len = 10
        sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head( how_rank_len )
        euclidean_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
        
        # 맨해튼
        # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
        # 추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
        seq_series = manhattan_dist_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
        rank_sorted_series = manhattan_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1).sort_values(ascending=False,)
        
        # 상위 기사 선택 후 보여주기
        how_rank_len = 10
        sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head( how_rank_len )
        manhattan_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
        """

        # 3개 합산
        # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
        # 추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
        seq_series = cosine_sim_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
        rank_sorted_series = ( cosine_sim_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1) 
                                + euclidean_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1) 
                                + manhattan_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1)
                                ).sort_values( ascending=False,)
        
        # 상위 기사 선택 후 보여주기
        how_rank_len = 10
        sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head( how_rank_len )
        total_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
        total_final_columns = ', '.join(total_final.columns)
        
        # 데이터베이스에 데이터 삽입
        for index, row in total_final.iterrows():
            # print (row)  # type : <class 'pandas.core.series.Series'>
            # break
            delete_query = f'''
                DELETE FROM stock_Korean_by_ESG_BackData.compared_articles
                WHERE article_reg_date = '{row["article_reg_date"]}'
                  AND company_name = '{row["company_name"]}'
            '''
            cursor.execute(delete_query)
            conn.commit()

            insert_query = f'''
                INSERT INTO stock_Korean_by_ESG_BackData.compared_articles
                (articles_id, company_name, article_reg_date, title, article_text, load_date)
                VALUES
                ({row["seq"]}, '{row["company_name"]}', '{row["article_reg_date"]}', '{row["title"]}', '{row["article_text"]}', NOW())
            '''
            # print (insert_query)
            # break  # for debug
            cursor.execute(insert_query)
            conn.commit()

        cursor.close()
        conn.close()

        cf.send_message("KOR", "compare scoring success by score_dataframes.dill")
        print("비교 스코어링 데이터 저장 successfully.")