from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import CrawlingArticle.CrawlingFunction as crawl
import ScoringByBert.bert_scoring as bert_scoring
import ScoringByKeyword.keyword_scoring as keyword_scoring


def DailyMain():
    '''
    # 종목정보 저장 (csv 파일은 일단 수동 저장필요함)
    cf.insert_Korean_all_code_info_to_DB()
    '''

    conn = cf.connect_to_db()
    cursor = conn.cursor()

    query = f'''
        DELETE FROM stock_Korean_by_ESG_BackData.news_articles
            WHERE article_reg_date = '{crawling_date_id.replace(".","-")}'
            AND search_keyword = '{query}'
            AND company_name = '{query.split("+")[0]}'
    '''
    # 네이버 뉴스기사 crawling
    querys = ["토스+이승건"]
    start_date = "2022.07.02"
    end_date = "2022.07.01"
    is_public = True
    crawl.crawling_articles_from_keyword(querys, start_date, end_date, is_public)
    
    """
    # bert 스코어링 결과 저장
    bert_scoring.main()
    
    # keyword로 스코어링
    keyword_scoring.main()
    """


if __name__ == '__main__':
    DailyMain()
