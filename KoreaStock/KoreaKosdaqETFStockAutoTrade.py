import time
import sys
import CommonFunction as cf
import ETFStockList as ETFStockList
import datetime
from pytz import timezone

# 자동매매 시작


def KoreaKosdaqETFStockAutoTrade():
    try:
        cf.send_message(
            "KOR", f"TODAY's ETF_symbol_list is {ETFStockList.ETF_symbol_list}")
        kosdaq150_sym = "229200"
        kosdaq150_name = ETFStockList.ETF_symbol_list[kosdaq150_sym]
        balance_cash = cf.get_balance("KOR")
        cf.send_message("KOR", f"주문 가능 현금 잔고: {balance_cash}원")
        cf.send_message("KOR", "===국내 주식 KOSDAQ ETF 자동매매 프로그램을 시작합니다===")

        while True:
            Korea_now = datetime.datetime.now(
                timezone('Asia/Seoul'))  # 한국 기준 현재 시간
            Korea_weekday = Korea_now.weekday()  # 한국 기준 현재 요일
            # buy : 월요일 시가, 화요일 종가, 수요일 종가
            if (Korea_weekday == 0 and Korea_now.hour == 9 and Korea_now.minute > 5) or (Korea_weekday in (1, 2) and Korea_now.hour == 15 and Korea_now.minute > 25):
                current_price = cf.get_current_price("KOR", kosdaq150_sym)
                balance_cash = cf.get_balance("KOR")
                buy_qty = int(balance_cash // current_price)  # 보유 현금 // 현재가격
                cf.buy("KOR", (kosdaq150_sym, kosdaq150_name), buy_qty, current_price)
                sys.exit(-1)
            # sell : 화요일 시가, 수요일 시가, 목요일 시가
            if Korea_weekday in (1, 2, 3) and Korea_now.hour == 9 and Korea_now.minute > 5:
                stock_dict = cf.get_stock_balance("KOR")  # 보유 주식 조회
                for sym, qty in stock_dict.items():
                    if sym == kosdaq150_sym:
                        current_price = cf.get_current_price("KOR", sym)
                        cf.sell("KOR", sym, qty, current_price)
                        sys.exit(-1)
            time.sleep(30)

    except Exception as e:
        cf.send_message("KOR", f"국내 KOSDAQ ETF [오류 발생]{e}")
        time.sleep(1)
