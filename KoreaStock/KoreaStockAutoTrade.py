from datetime import datetime, timedelta
from pytz import timezone
import sys
import time
import CommonFunction as cf
import ETFStockList as ETFStockList


# 자동매매 시작
def KoreaStockAutoTrade(Korea_start, Korea_end, symbol_list):
    try:
        bought_list = []  # 매수 완료된 종목 리스트
        stock_dict = cf.get_stock_balance("KOR")  # 보유 주식 조회
        for sym in stock_dict.keys():
            if sym not in ETFStockList.ETF_symbol_list.keys():
                bought_list.append(sym)
        print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
        target_buy_count = 5  # 매수할 종목 수
        buy_percent = 0.2  # 종목당 매수 금액 비율
        send_msg = False
        fixed_balance_cash = cf.get_balance("KOR")
        cf.send_message("KOR", f"주문 가능 현금 잔고: {fixed_balance_cash}원")
        cf.send_message("KOR", "===국내 주식 자동매매 프로그램을 시작합니다===")
        today_black_list = []
        while True:
            t_now = datetime.now(timezone('Asia/Seoul'))  # 서울 기준 현재 시간
            Korea_weekday = t_now.weekday()  # 한국 기준 현재 요일
            if Korea_weekday == 0:
                cf.send_message("KOR", "Today is Monday!! Monday is closed for general market.")
                sys.exit(-1)
            t_start = Korea_start + timedelta(minutes=10)
            t_start2 = Korea_start + timedelta(minutes=15)
            t_sell = Korea_end + timedelta(minutes=-10)
            # 장시작+10분에 보유 종목이 있으면 : 잔여 수량 매도
            if t_start < t_now < t_start2 and len(bought_list) > 0:
                print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
                cf.send_message("KOR", "===국내 주식 잔여 수량 매도===")
                for sym, qty in stock_dict.items():
                    if sym not in ETFStockList.ETF_symbol_list.keys():
                        cf.sell("KOR", sym, qty, "0")
                bought_list = []
                print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
            if t_start2 < t_now < t_sell:  # 장시작+15분~장종료-10분 : 매수
                print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
                sell_stock = cf.get_stock_balance_for_check("KOR")
                sell_stock = list(set(sell_stock))
                if len(sell_stock) > 0:
                    today_black_list.extend(sell_stock)
                    today_black_list = list(set(today_black_list))
                    for stock in sell_stock:
                        print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
                        bought_list.remove(stock)
                        print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
                for sym in symbol_list.items():
                    print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
                    if len(bought_list) < target_buy_count:
                        if sym[0] in bought_list or sym[0] in today_black_list or sym[0] in ETFStockList.ETF_symbol_list.keys():
                            continue
                        target_price = cf.get_target_price("KOR", sym[0])
                        current_price = cf.get_current_price("KOR", sym[0])
                        if target_price < current_price:
                            buy_qty = 0  # 매수할 수량 초기화
                            # 매도주문이후 늦게팔리는 경우 보유 현금이 달라지므로 매번 계산
                            balance_cash = cf.get_balance("KOR")
                            if balance_cash < int(fixed_balance_cash * buy_percent):
                                continue
                            # 보유 현금 * 종목당 비율 (종목별 주문금액) // 현재가격
                            buy_qty = int(fixed_balance_cash * buy_percent // current_price)
                            if buy_qty > 0:
                                cf.send_message("KOR", f"국내 주식 {sym[1]} (={sym[0]}) 목표가 달성({target_price} < {current_price}) 매수를 시도합니다.")
                                result = cf.buy("KOR", sym, buy_qty, current_price)
                                if result:
                                    print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
                                    bought_list.append(sym[0])
                                    print([time.strftime('%Y-%m-%d %H:%M:%S')], "chk!!", bought_list)
                                    cf.get_stock_balance("KOR")
                                    balance_cash = cf.get_balance("KOR")
                                    cf.send_message("KOR", f"주문 가능 현금 잔고: {balance_cash}원")
                if t_now.minute % 10 == 0 and send_msg == False:
                    cf.get_stock_balance("KOR")
                    send_msg = True
                if t_now.minute % 10 == 1:
                    send_msg = False
            # 장종료-10분~에 보유 종목이 있으면 : 일괄 매도
            if t_sell < t_now and len(bought_list) > 0:
                stock_dict = cf.get_stock_balance("KOR")
                for sym, qty in stock_dict.items():
                    if sym not in ETFStockList.ETF_symbol_list.keys():
                        cf.sell("KOR", sym, qty, "0")
                bought_list = []
            time.sleep(1)
    except Exception as e:
        cf.send_message("KOR", f"국내 주식 [오류 발생]{e}")
        time.sleep(1)
