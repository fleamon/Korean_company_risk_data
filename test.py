import dill
import pandas as pd
   
# dill 파일에서 데이터 읽기
with open('./dill_files/20240215_0038_score_dataframes.dill', 'rb') as f:
    data = dill.load(f)

print (data)
print ()

data_column_names = ', '.join(data.columns)
print("데이터프레임의 컬럼: ", data_column_names)

print ()
print ()

with open('/Users/kakao/Applications/MyVSCodeWorkspace/stock_Korean_by_ESG_ForData/ScoringByModel/dill_files/20240215_0038.dill', 'wb') as f:
    dill.dump(data, f, protocol=dill.HIGHEST_PROTOCOL)

with open('/Users/kakao/Applications/MyVSCodeWorkspace/stock_Korean_by_ESG_ForData/ScoringByModel/dill_files/20240215_0038.dill', 'rb') as f:
    loaded_data = dill.load(f)

print(loaded_data)
print ()

loaded_data_column_names = ', '.join(loaded_data.columns)
print("데이터프레임의 컬럼: ", loaded_data_column_names)

print ()
print ()

# data와 loaded_data가 모두 데이터프레임인지 확인
if isinstance(data, pd.DataFrame) and isinstance(loaded_data, pd.DataFrame):
    # 두 데이터프레임이 동일한지 확인
    if data.equals(loaded_data):
        print("data와 loaded_data는 동일합니다.")
    else:
        print("data와 loaded_data는 동일하지 않습니다.")
else:
    print("data와 loaded_data가 모두 데이터프레임이 아닙니다.")

print ()
print ()
