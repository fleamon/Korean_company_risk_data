from datetime import datetime
from pytz import timezone
import time
import CommonFunction as cf
import ETFStockList as ETFStockList

# 자동매매 시작
def KoreaETFStockBackData():
    try:
        cf.send_message("KOR", f"TODAY's ETF BackData INSERT START!")
        is_first = True
        # 종목
        kodex200_sym = "069500"
        kodex200_name = ETFStockList.ETF_symbol_list[kodex200_sym]
        kodex200_inverse_sym = "252670"
        kodex200_inverse_name = ETFStockList.ETF_symbol_list[kodex200_inverse_sym]
        kodex200_leverage_sym = "122630"
        kodex200_leverage_name = ETFStockList.ETF_symbol_list[kodex200_leverage_sym]
        kodex_kosdaq_150_sym = "229200"
        kodex_kosdaq_150_name = ETFStockList.ETF_symbol_list[kodex_kosdaq_150_sym]
        kodex_kosdaq_150_inverse_sym = "251340"
        kodex_kosdaq_150_inverse_name = ETFStockList.ETF_symbol_list[kodex_kosdaq_150_inverse_sym]
        kodex_kosdaq_150_leverage_sym = "233740"
        kodex_kosdaq_150_leverage_name = ETFStockList.ETF_symbol_list[kodex_kosdaq_150_leverage_sym]

        while True:
            kodex200_infos = cf.get_current_investor(kodex200_sym)  # KODEX 200 주식현재가 투자자 확인
            kodex200_inverse_infos = cf.get_current_investor(kodex200_inverse_sym)  # KODEX 200선물인버스2X 주식현재가 투자자 확인
            kodex200_leverage_infos = cf.get_current_investor(kodex200_leverage_sym)  # KODEX 레버리지 주식현재가 투자자 확인
            kodex_kosdaq_150_infos = cf.get_current_investor(kodex_kosdaq_150_sym)  # KODEX kosdaq 150 주식현재가 투자자 확인
            kodex_kosdaq_150_inverse_infos = cf.get_current_investor(kodex_kosdaq_150_inverse_sym)  # KODEX kosdaq 150 선물인버스 주식현재가 투자자 확인
            kodex_kosdaq_150_leverage_infos = cf.get_current_investor(kodex_kosdaq_150_leverage_sym)  # KODEX kosdaq 150 레버리지 주식현재가 투자자 확인
            # 하루에 한번은 최근 30일 데이터 모두 delete -> insert
            if is_first == True:
                # kospi
                for kodex200_info in kodex200_infos:
                    cf.insert_to_aws_db_kospi200_etf_investor_info(kodex200_sym, kodex200_name, kodex200_info)
                for kodex200_inverse_info in kodex200_inverse_infos:
                    cf.insert_to_aws_db_kospi200_etf_investor_info(kodex200_inverse_sym, kodex200_inverse_name, kodex200_inverse_info)
                for kodex200_leverage_info in kodex200_leverage_infos:
                    cf.insert_to_aws_db_kospi200_etf_investor_info(kodex200_leverage_sym, kodex200_leverage_name, kodex200_leverage_info)
                # kosdaq
                for kodex_kosdaq_150_info in kodex_kosdaq_150_infos:
                    cf.insert_to_aws_db_kospi200_etf_investor_info(kodex_kosdaq_150_sym, kodex_kosdaq_150_name, kodex_kosdaq_150_info)
                for kodex_kosdaq_150_inverse_info in kodex_kosdaq_150_inverse_infos:
                    cf.insert_to_aws_db_kospi200_etf_investor_info(kodex_kosdaq_150_inverse_sym, kodex_kosdaq_150_inverse_name, kodex_kosdaq_150_inverse_info)
                for kodex_kosdaq_150_leverage_info in kodex_kosdaq_150_leverage_infos:
                    cf.insert_to_aws_db_kospi200_etf_investor_info(kodex_kosdaq_150_leverage_sym, kodex_kosdaq_150_leverage_name, kodex_kosdaq_150_leverage_info)
                is_first = False
            # 계속해서 delete -> insert 하는건 마지막 날짜만! ([0])
            else:
                # kospi
                cf.insert_to_aws_db_kospi200_etf_investor_info(kodex200_sym, kodex200_name, kodex200_infos[0])
                cf.insert_to_aws_db_kospi200_etf_investor_info(kodex200_inverse_sym, kodex200_inverse_name, kodex200_inverse_infos[0])
                cf.insert_to_aws_db_kospi200_etf_investor_info(kodex200_leverage_sym, kodex200_leverage_name, kodex200_leverage_infos[0])
                # kosdaq
                cf.insert_to_aws_db_kospi200_etf_investor_info(kodex_kosdaq_150_sym, kodex_kosdaq_150_name, kodex_kosdaq_150_infos[0])
                cf.insert_to_aws_db_kospi200_etf_investor_info(kodex_kosdaq_150_inverse_sym, kodex_kosdaq_150_inverse_name, kodex_kosdaq_150_inverse_infos[0])
                cf.insert_to_aws_db_kospi200_etf_investor_info(kodex_kosdaq_150_leverage_sym, kodex_kosdaq_150_leverage_name, kodex_kosdaq_150_leverage_infos[0])
            time.sleep(10800)  # 3시간마다 거래량 delete -> insert
    except Exception as e:
        cf.send_message("KOR", f"국내 ETF BackData [오류 발생]{e}")
        time.sleep(1)