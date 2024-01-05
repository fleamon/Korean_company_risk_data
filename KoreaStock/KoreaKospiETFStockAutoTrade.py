import time
import sys
import CommonFunction as cf
import ETFStockList as ETFStockList

# 자동매매 시작
def KoreaKospiETFStockAutoTrade():
    try:
        cf.send_message("KOR", f"TODAY's ETF_symbol_list is {ETFStockList.ETF_symbol_list}")
        kodex200_sym = "069500"
        kodex200_inverse_sym = "252670"
        kodex200_leverage_sym = "122630"
        balance_cash = cf.get_balance("KOR")
        cf.send_message("KOR", f"주문 가능 현금 잔고: {balance_cash}원")
        cf.send_message("KOR", "===국내 주식 KOSPI ETF 자동매매 프로그램을 시작합니다===")
        buy_percent = 0.5  # 매수 금액 비율

        # 오늘 살 종목확인
        today_buy_item_info = cf.select_from_aws_db_kospi200_etf_investor_info_today_item()[0]
        today_buy_item_code = today_buy_item_info[0]
        today_buy_item_name = today_buy_item_info[1]
        if today_buy_item_code == '-':
            cf.send_message("KOR", "TODAY's KOSPI ETF trade is NOT OPERATE. Because of IMPOSSIBLE JUDGMENT!!")
            sys.exit(-1)
        # KODEX 200 살때는 레버리지로!
        # if today_buy_item_info[0] == kodex200_sym:
        #     today_buy_item_code = kodex200_leverage_sym
        #     today_buy_item_name = ETFStockList.ETF_symbol_list[kodex200_leverage_sym]
        is_sell_command = False

        while True:
            stock_dict = cf.get_stock_balance("KOR")  # 보유 주식 조회
            if today_buy_item_code in stock_dict.keys():  # 오늘 살 KOSPI ETF 종목이 이미 가지고있는 종목이다!
                cf.send_message("KOR", "TODAY's KOSPI ETF trade is NOT OPERATE. Because of EQUAL!!")
                sys.exit(-1)
            else:  # 보유 KOSPI ETF가 없거나, 있지만 갈아타야하는 상황!
                if kodex200_sym not in stock_dict.keys() and kodex200_inverse_sym not in stock_dict.keys() and kodex200_leverage_sym not in stock_dict.keys():  # 보유 주식에 KOSPI ETF가 비었다! 오늘 추천 사고 끝!
                    current_price = cf.get_current_price("KOR", today_buy_item_code)
                    balance_cash = cf.get_balance("KOR")
                    buy_qty = int(balance_cash * buy_percent // current_price)  # 보유 현금 * 종목당 비율 (종목별 주문금액) // 현재가격
                    cf.buy("KOR", (today_buy_item_code, today_buy_item_name), buy_qty, "0")
                    sys.exit(-1)
                else:  # 보유 KOSPI ETF가 있지만 갈아타야하는 상황!
                    if is_sell_command == True:  # 매도 주문 했으면?
                        time.sleep(300)  # 매도주문했으면 5분 기다리기
                        continue
                    else:  # is_sell_command == False:  # 매도 주문 안했으면?
                        for sym, qty in stock_dict.items():
                            if sym in (kodex200_sym, kodex200_inverse_sym, kodex200_leverage_sym):
                                cf.sell("KOR", sym, qty, "0")
                                is_sell_command = True
            time.sleep(1)
        
    except Exception as e:
        cf.send_message("KOR", f"국내 KOSPI ETF [오류 발생]{e}")
        time.sleep(1)