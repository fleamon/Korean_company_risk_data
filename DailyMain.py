from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import CrawlingArticle.CrawlingFunction as crawl
import ScoringByBert.bert_scoring as bert_scoring
import ScoringByKeyword.keyword_scoring as keyword_scoring
import CompareByKeyword.compared_articles as compared_articles
import SummarizeArticles.summaize_articles as summaize_articles
import dart.dart as dart

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
    # company_ceo_name_list = ['카카오+김범수', '삼성전자+이재용', '현대자동차+정의선', 'LG+구광모', 'SK+최태원']
    company_ceo_name_list = ['카카오+김범수']
    # company_ceo_name_list = ['삼성전자+이재용']
    for company_ceo_name in company_ceo_name_list:
        start_date = "2023.10.25"
        end_date = "2023.10.01"
        # end_date = "2010.01.01"
        is_public = True

        # article crawling (완료)
        # crawl.crawling_articles_from_keyword(company_ceo_name, formatted_yesterday, formatted_yesterday, is_public)
        # crawl.crawling_articles_from_keyword(company_ceo_name, start_date, end_date, is_public)

        # dart 공시자료 추가
        # dart.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        dart.main(company_ceo_name, start_date, end_date)

        # 키워드비교 스코어링 (완료)
        # compare_scoring.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        # compared_articles.main(company_ceo_name, start_date, end_date)

        # # 특정날짜 특정기업 기사 한줄요약
        # # summaize_articles.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        # summaize_articles.main(company_ceo_name, start_date, end_date)

    # bert 스코어링 결과 저장 (param 필요없음(dill file))
    # bert_scoring.main()
    
    # keyword로 스코어링 결과 저장 (param 필요없음(dill file))
    # keyword_scoring.main()


if __name__ == '__main__':
    DailyMain()
