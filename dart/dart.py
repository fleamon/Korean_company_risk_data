import dart_fss as dart_fss
import pandas as pd

dart_fss.set_api_key(api_key='c1cb1a485a7771762be34d8caa61880dbfd50e9d')

# print (dart_fss)

corp_list = dart_fss.get_corp_list()
# print (corp_list.corps)

all = dart_fss.api.filings.get_corp_code()
# print (all)

df = pd.DataFrame(all)
# print (df)

# 상장
df_listed = df[df['stock_code'].notnull()]
print (df_listed)

# 비상장
df_non_listed = df[df['stock_code'].isnull()]
print (df_non_listed)

# df_listed.to_excel('public_item.xlsx')
# df_non_listed.to_excel('not_public_item.xlsx')


corp_code = df_listed[df_listed['corp_name'] == '카카오'].iloc[0,0]
print (dart_fss.api.filings.get_corp_info(corp_code))
