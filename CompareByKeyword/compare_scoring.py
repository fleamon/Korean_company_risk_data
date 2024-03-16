import dill
import pandas as pd
import CommonFunction as cf
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances, manhattan_distances


def main():
    # 해당 일과 기업
    impact_date = pd.to_datetime('2023-10-20', format='%Y-%m-%d').date()
    # print ("impact_date")
    # print (impact_date)
    impact_firm = '카카오'
    # print ("impact_firm")
    # print (impact_firm)
    
    with open('./dill_files/20240215_0038_score_dataframes.dill', 'rb') as f:
        data = dill.load(f)

    tfidf_vectorizer = TfidfVectorizer()
    # print ("tfidf_vectorizer")
    # print (tfidf_vectorizer)
    tfidf_matrix = tfidf_vectorizer.fit_transform(data['article_text'])
    # print ("tfidf_matrix")
    # print (tfidf_matrix)

    cosine_similarities = cosine_similarity(tfidf_matrix, tfidf_matrix)
    # print ("cosine_similarities")
    # print (cosine_similarities)
    euclidean_distances_matrix = euclidean_distances(tfidf_matrix, tfidf_matrix)
    # print ("euclidean_distances_matrix")
    # print (euclidean_distances_matrix)
    manhattan_distances_matrix = manhattan_distances(tfidf_matrix, tfidf_matrix)
    # print ("manhattan_distances_matrix")
    # print (manhattan_distances_matrix)

    cosine_sim_df = pd.DataFrame(cosine_similarities,  
                                columns = data['seq'] , 
                                index = pd.MultiIndex.from_frame( data[['seq', 'article_reg_date', 'company_name',]] ) , 
                                )
    # print ("cosine_sim_df")
    # print (cosine_sim_df)

    # rank로 변환
    cosine_sim_rank_df = cosine_sim_df.rank(axis=1, method='min')
    # print ("cosine_sim_rank_df")
    # print (cosine_sim_rank_df)
    euclidean_dist_df = pd.DataFrame(euclidean_distances_matrix,  
                                columns = data['seq'] , 
                                index = pd.MultiIndex.from_frame( data[['seq', 'article_reg_date', 'company_name',]] ) , 
                                )
    # print ("euclidean_dist_df")
    # print (euclidean_dist_df)
    # 거리 짧을수록 높은 점수
    euclidean_dist_rank_df = euclidean_dist_df.rank(axis=1, ascending = False)
    # print ("euclidean_dist_rank_df")
    # print (euclidean_dist_rank_df)
    manhattan_dist_df = pd.DataFrame(manhattan_distances_matrix,  
                                columns = data['seq'] , 
                                index = pd.MultiIndex.from_frame( data[['seq', 'article_reg_date', 'company_name',]] ) , 
                                )
    # print ("manhattan_dist_df")
    # print (manhattan_dist_df)
    #거리 짧을수록 높은 점수
    manhattan_dist_rank_df = manhattan_dist_df.rank(axis=1, ascending = False)
    # print ("manhattan_dist_rank_df")
    # print (manhattan_dist_rank_df)

    """
    # 코사인
    # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
    #추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
    seq_series = cosine_sim_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
    # print ("seq_series")
    # print (seq_series)
    rank_sorted_series = cosine_sim_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1).sort_values(ascending=False,)
    # print ("rank_sorted_series")
    # print (rank_sorted_series)

    # 상위 기사 선택 후 보여주기
    how_rank_len = 10
    sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head(how_rank_len)
    # print ("sim_seq_list")
    # print (sim_seq_list)
    cosine_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
    # print ("cosine_final")
    # print (cosine_final)

    # 유클리드
    # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
    #추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
    seq_series = euclidean_dist_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
    # print ("seq_series")
    # print (seq_series)
    rank_sorted_series = euclidean_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1).sort_values(ascending=False,)
    # print ("rank_sorted_series")
    # print (rank_sorted_series)

    # 상위 기사 선택 후 보여주기
    how_rank_len = 10
    sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head( how_rank_len )
    # print ("sim_seq_list")
    # print (sim_seq_list)
    euclidean_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
    # print ("euclidean_final")
    # print (euclidean_final)

    # 맨해튼
    # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
    #추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
    seq_series = manhattan_dist_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
    # print ("seq_series")
    # print (seq_series)
    rank_sorted_series = manhattan_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1).sort_values(ascending=False,)
    # print ("rank_sorted_series")
    # print (rank_sorted_series)

    # 상위 기사 선택 후 보여주기
    how_rank_len = 10
    sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head( how_rank_len )
    # print ("sim_seq_list")
    # print (sim_seq_list)
    manhattan_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
    # print ("manhattan_final")
    # print (manhattan_final)
    """

    # 3개 합산
    # 해당 일에 뉴스들과 유사도가 높은 뉴스 확인 (rank 클수록 더 유사도 높음)
    # 추가로 해당 기업 제외 뉴스들 + 해당 일자 제외( 해당일에 다른거로 너무 많이 작성함.)
    seq_series = cosine_sim_rank_df.query("company_name == @impact_firm and article_reg_date == @impact_date").index.to_frame(index = False)['seq']
    # print ("seq_series")
    # print (seq_series)
    rank_sorted_series = ( cosine_sim_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1) 
                            + euclidean_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1) 
                            + manhattan_dist_rank_df.query("company_name != @impact_firm and article_reg_date != @impact_date")[seq_series].sum(axis = 1)
                            ).sort_values( ascending=False,)
    # print ("rank_sorted_series")
    # print (rank_sorted_series)

    # 상위 기사 선택 후 보여주기
    how_rank_len = 10
    sim_seq_list = rank_sorted_series.index.to_frame()['seq'].head( how_rank_len )
    # print ("sim_seq_list")
    # print (sim_seq_list)
    total_final = data.query('seq in @sim_seq_list')[['seq', 'company_name', 'article_reg_date', 'title', 'article_text']]
    # print ("total_final")
    print (total_final)
    print (type(total_final))
    print (len(total_final))
    total_final_columns = ', '.join(total_final.columns)
    print (total_final_columns)
    print (type(total_final_columns))
    print (len(total_final_columns))

    conn = cf.connect_to_db()
    cursor = conn.cursor()

    # MySQL 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_Korean_by_ESG_BackData.articles_compare_scoring (
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

    # truncate 테이블
    truncate_query = """TRUNCATE TABLE stock_Korean_by_ESG_BackData.articles_compare_scoring"""
    cursor.execute(truncate_query)
    conn.commit()
    
    # 데이터베이스에 데이터 삽입
    for index, row in total_final.iterrows():
        print (row)  # type : <class 'pandas.core.series.Series'>
        # break
        insert_query = f'''
            INSERT INTO stock_Korean_by_ESG_BackData.articles_compare_scoring
            (articles_id, company_name, article_reg_date, title, article_text, load_date)
            VALUES
            ({row["seq"]}, '{row["company_name"]}', '{row["article_reg_date"]}', '{row["title"]}', '{row["article_text"]}', NOW())
        '''
        print (insert_query)
        # break  # for debug
        cursor.execute(insert_query)
        conn.commit()

    cursor.close()
    conn.close()

    cf.send_message("KOR", "compare scoring success by score_dataframes.dill")
    print("비교 스코어링 데이터 저장 successfully.")