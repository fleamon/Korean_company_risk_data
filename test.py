import CommonFunction as cf
import time

# item_code_list = ["101T06", "101T12", "306V03035"]  # 실시간 체결가 또는 엑셀로 export 하고싶은 선물옵션코드
# df = cf.get_domestic_future_master_dataframe(item_code_list)
# print ([time.strftime('%Y-%m-%d %H:%M:%S')], df)
# print ([time.strftime('%Y-%m-%d %H:%M:%S')], "Done")

# df = cf.get_domestic_stk_future_master_dataframe()
# print ([time.strftime('%Y-%m-%d %H:%M:%S')], "Done")

# item_code_list = ["ESM23"]  # 실시간 체결가 또는 엑셀로 export 하고싶은 선물옵션코드
# cf.get_overseas_future_master_dataframe(item_code_list)  # 엑셀로 export

# test 중 !!!! get_stock_balance
# cf.get_stock_balance("KOR")
# cf.get_Korean_stock_dict()
# candidate_dict = cf.get_Korean_total_amount_rank_bottom_20_percent()
# i = 1
# for key, val in candidate_dict.items():
#     print (i, key, val)
#     # print (val)
#     i += 1

fixed_balance_cash = cf.get_balance("KOR")
print (f"주문 가능 현금 잔고: {fixed_balance_cash}원")