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
    company_ceo_name_list = ['카카오+김범수', '삼성전자+이재용', '현대자동차+정의선', 'LG+구광모', 'SK+최태원']
    for company_ceo_name in company_ceo_name_list:
        start_date = "2024.03.27"
        end_date = "2024.03.20"
        is_public = True
        # crawl.crawling_articles_from_keyword(company_ceo_name, formatted_yesterday, formatted_yesterday, is_public)
        crawl.crawling_articles_from_keyword(company_ceo_name, start_date, end_date, is_public)
        # 특정날짜 특정기업 기사 한줄요약
        # summaize_articles.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        # summaize_articles.main(company_ceo_name, start_date, end_date)
    
    '''
    # bert 스코어링 결과 저장
    bert_scoring.main()
    
    # keyword로 스코어링
    keyword_scoring.main()
    
    # 키워드비교 스코어링
    compare_scoring.main()
    '''


if __name__ == '__main__':
    DailyMain()
