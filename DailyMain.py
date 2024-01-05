from datetime import datetime, timedelta
import time
import sys
from pytz import timezone

import CommonFunction as cf
import KoreaStock.KoreaStockAutoTrade as KoreaStockAutoTrade
import KoreaStock.KoreaStockList as KoreaList


def stock_Korean():
    Korea_now = datetime.now(timezone('Asia/Seoul'))  # 한국 기준 현재 시간
    Korea_weekday = Korea_now.weekday()  # 한국 기준 현재 요일
    Korea_start = Korea_now.replace(hour=9, minute=0, second=0, microsecond=0)  # 한국 장 시작시간
    Korea_end = Korea_now.replace(hour=15, minute=30, second=0, microsecond=0)  # 한국 장 끝 시간
    Korean_date = Korea_now.strftime('%Y-%m-%d')
    Is_abnormal = cf.get_abnormal_date_array(Korean_date, "한국")  # 오늘 해당국가의 상태확인
    if ":" in Is_abnormal:
        Korea_end = Korea_now.replace(hour=int(Is_abnormal.split(":")[0]), minute=int(Is_abnormal.split(":")[1]), second=0, microsecond=0)  # 조기종료 한국 장 끝 시간
    if Korea_weekday in (5, 6) or Is_abnormal == "abnormal":  # 주말 or 장휴일
        cf.send_message("KOR", "In Korea, Today is not open.")
        sys.exit(-1)
    else:  # 장 열리는 날
        # 종목 update (관심 + 컨센서스크롤링)
        symbol_list = KoreaList.symbol_list
        symbol_list.update(cf.get_Korean_stock_dict())
        cf.send_message("KOR", f"TODAY's symbol_list is {symbol_list}")
        while True:
            Korea_now = datetime.now(timezone('Asia/Seoul'))  # 한국 기준 현재 시간
            if Korea_now > Korea_start + timedelta(minutes=10):
                print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : KOR main !!!!!")
                KoreaStockAutoTrade.KoreaStockAutoTrade(Korea_start, Korea_end, symbol_list)
            else:
                time.sleep(60)


if __name__ == '__main__':
    stock_Korean()
