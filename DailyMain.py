from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import ScoringByModel.after_scoring as after_scoring


def DailyMain():
    # 종목정보 저장 (csv 파일은 일단 수동 저장필요함)
    # cf.insert_Korean_all_code_info_to_DB()
    
    # 뉴스기사 crawling
    querys = ["토스+이승건"]
    start_date = "2023.06.30"
    end_date = "2022.07.01"
    is_public = True
    # cf.crawling_articles_from_keyword(querys, start_date, end_date, is_public)

    # bert 스코어링
    after_scoring


if __name__ == '__main__':
    DailyMain()
