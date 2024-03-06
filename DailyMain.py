from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import CrawlingArticle.CrawlingFunction as crawl
import ScoringByBert.bert_scoring as bert_scoring
import ScoringByKeyword.keyword_scoring as keyword_scoring


def DailyMain():
    yesterday = datetime.now() - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%Y.%m.%d')

    '''
    # 종목정보 저장 (csv 파일은 일단 수동 저장필요함)
    cf.insert_Korean_all_code_info_to_DB()
    '''

    # 뉴스기사 crawling (네이버, 다음)
    company_ceo_name_list = crawl.get_company_ceo_name()
    company_ceo_name_list = company_ceo_name_list[0:10]
    for company_ceo_name in company_ceo_name_list:
        # start_date = "2022.07.02"
        # end_date = "2022.07.01"
        is_public = True
        crawl.crawling_articles_from_keyword(company_ceo_name, formatted_yesterday, formatted_yesterday, is_public)

    """
    # bert 스코어링 결과 저장
    bert_scoring.main()
    
    # keyword로 스코어링
    keyword_scoring.main()
    """


if __name__ == '__main__':
    DailyMain()
