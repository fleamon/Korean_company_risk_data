from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import CrawlingArticle.CrawlingFunction as crawl
import ScoringByBert.bert_scoring as bert_scoring
import ScoringByKeyword.keyword_scoring as keyword_scoring
import CompareByKeyword.compare_scoring as compare_scoring
import SummarizeArticles.summaize_articles as summaize_articles


def DailyMain():
    yesterday = datetime.now() - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%Y.%m.%d')

    '''
    # 종목정보 저장 (csv 파일은 일단 수동 저장필요함)
    cf.insert_Korean_all_code_info_to_DB()
    '''

    # 뉴스기사 crawling (네이버, 다음)
    # company_ceo_name_list = cf.get_company_ceo_name()
    # company_ceo_name_list = company_ceo_name_list[0:50]  # test
    company_ceo_name_list = ['카카오+김범수', '삼성전자+이재용']
    for company_ceo_name in company_ceo_name_list:
        start_date = "2024.03.16"
        end_date = "2024.03.10"
        is_public = True
        # crawl.crawling_articles_from_keyword(company_ceo_name, formatted_yesterday, formatted_yesterday, is_public)
        crawl.crawling_articles_from_keyword(company_ceo_name, start_date, end_date, is_public)

    '''
    # bert 스코어링 결과 저장
    bert_scoring.main()
    
    # keyword로 스코어링
    keyword_scoring.main()
    
    # 키워드비교 스코어링
    compare_scoring.main()
    '''

    # 특정날짜 기사들 한줄요약
    summaize_articles.main()


if __name__ == '__main__':
    DailyMain()
