from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import Score_by_BERT as score


def DailyMain():
    # cf.insert_Korean_all_code_info_to_DB()  # 종목정보 저장 (csv 파일은 일단 수동 저장필요함)
    # cf.crawling_articles_from_keyword()  # 뉴스기사 crawling
    score


if __name__ == '__main__':
    DailyMain()
