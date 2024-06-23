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
import dart.dart_corp_list as dart_corp_list

def DailyMain():
    yesterday = datetime.now() - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%Y.%m.%d')

    # 상장 종목정보 저장
    # dart_corp_list.main()

    # 뉴스기사 crawling (네이버, 다음)
    # company_ceo_name_list = cf.get_company_ceo_name()
    # company_ceo_name_list = company_ceo_name_list[0:50]  # test
    # company_ceo_name_list = ['카카오+김범수', '삼성전자+이재용', '현대자동차+정의선', 'LG+구광모', 'SK+최태원']
    # company_ceo_name_list = ['카카오+김범수']
    company_ceo_name_list = ['삼성전자+이재용']
    # company_ceo_name_list = ['한진+조원태', '대한항공+조원태', '한진칼+조원태']
    # company_ceo_name_list = ['한진칼,한진,대한항공+조원태']
    # company_ceo_name_list = ['하이브+방시혁']
    # company_ceo_name_list = ['포스코+장인화']
    for company_ceo_name in company_ceo_name_list:
        start_date = "2024.06.11"
        end_date = "2024.06.11"
        # end_date = "2024.05.01"
        # end_date = "2023.01.01"  # 하이브
        # end_date = "2024.01.01"  # 포스코
        is_public = True

        # article crawling (완료) (커뮤니티 점검필요)
        # crawl.crawling_articles_from_keyword(company_ceo_name, formatted_yesterday, formatted_yesterday, is_public)
        crawl.crawling_articles_from_keyword(company_ceo_name, start_date, end_date, is_public)

        # dart 공시자료 추가
        # start_date = "2020.11.30"
        # end_date = "2020.11.30"
        # dart.main(company_ceo_name, start_date, end_date)
        # start_date = "2020.11.30"
        # end_date = "2020.11.01"
        # dart.main(company_ceo_name, start_date, end_date)
        # start_date = "2020.03.31"
        # end_date = "2020.03.01"
        # dart.main(company_ceo_name, start_date, end_date)
        # start_date = "2019.05.31"
        # end_date = "2019.05.01"
        # dart.main(company_ceo_name, start_date, end_date)
        # start_date = "2019.02.28"
        # end_date = "2019.01.01"
        # dart.main(company_ceo_name, start_date, end_date)
        # start_date = "2018.11.30"
        # end_date = "2018.11.01"
        # dart.main(company_ceo_name, start_date, end_date)

        # # 특정날짜 특정기업 기사 한줄요약 (완료)
        # # summaize_articles.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        # summaize_articles.main(company_ceo_name, start_date, end_date)

        # # bert 스코어링 결과 저장 (완료)
        # # bert_scoring.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        # bert_scoring.main(company_ceo_name, start_date, end_date)
        
        # # keyword로 스코어링 결과 저장 (완료)
        # # keyword_scoring.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        # keyword_scoring.main(company_ceo_name, start_date, end_date)

        # # 키워드비교 스코어링 (완료)
        # # compare_scoring.main(company_ceo_name, formatted_yesterday, formatted_yesterday)
        # compared_articles.main(company_ceo_name, start_date, end_date)


if __name__ == '__main__':
    DailyMain()
