from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import Score_by_BERT


def DailyMain():
    # 종목정보 저장 (csv 파일은 일단 수동 저장필요함)
    # cf.insert_Korean_all_code_info_to_DB()
    
    # 뉴스기사 crawling
    # querys = ["카카오+김범수"]
    querys = ["삼성전자+이재용"]
    # querys = ["카카오+김범수", "삼성전자+이재용"]
    start_date = "2024.02.06"
    end_date = "2024.02.01"
    # cf.crawling_articles_from_keyword(querys, start_date, end_date)

    Score_by_BERT


if __name__ == '__main__':
    DailyMain()
