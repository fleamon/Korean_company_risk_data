import pymysql
import numpy as np

import pandas as pd
pd.set_option('mode.chained_assignment',  None)

from tqdm.notebook import trange, tqdm
import pandas as pd
import torch
from transformers import BertTokenizer, BertForSequenceClassification
from torch.nn.functional import softmax
import CommonFunction as cf


conn = cf.connect_to_db()
cursor = conn.cursor()
query = """SELECT * FROM stock_Korean_by_ESG_BackData.news_articles"""
cursor.execute(query)
result = cursor.fetchall()
news_text_df = pd.DataFrame(result)
cursor.close()
conn.close()

print (news_text_df)
"""
news_text_df
#news_text_df.to_excel("확인excel.xlsx")
news_text_df
# kr-finbert
## 스코어 산출
model_name = "snunlp/KR-FinBert"
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertForSequenceClassification.from_pretrained(model_name)

max_length = 64  # 적절한 길이로 조절
padding = 'max_length'
truncation = True
news_text_df['title_positive_score'] = np.nan
news_text_df['title_negative_score'] = np.nan
news_text_df['article_positive_score'] = np.nan
news_text_df['article_negative_score'] = np.nan
news_text_df['title'].iloc[4:5].tolist()
### 제목 스코어링
for article_idx in trange(len(news_text_df)):
#for article_idx in range(3):
    tokenized_texts = tokenizer(news_text_df['title'].iloc[article_idx : article_idx + 1].tolist(),
                             padding=padding,
                             truncation=truncation,
                             max_length=max_length,
                             return_tensors="pt")
    outputs = model(**tokenized_texts)
    scores = softmax(outputs.logits, dim=-1)
    news_text_df['title_positive_score'].iloc[article_idx] = scores[:, 1].detach().numpy()  # 긍정 클래스에 대한 스코어
    news_text_df['title_negative_score'].iloc[article_idx] = scores[:, 0].detach().numpy()  # 부정 클래스에 대한 스코어
 

### 내용 (전체) 스코어링
for article_idx in trange(len(news_text_df)):
#for article_idx in range(3):
    tokenized_texts = tokenizer(news_text_df['article_text'].iloc[article_idx : article_idx + 1].tolist(),
                             padding=padding,
                             truncation=truncation,
                             max_length=max_length,
                             return_tensors="pt")
    outputs = model(**tokenized_texts)
    scores = softmax(outputs.logits, dim=-1)
    news_text_df['article_positive_score'].iloc[article_idx] = scores[:, 1].detach().numpy()  # 긍정 클래스에 대한 스코어
    news_text_df['article_negative_score'].iloc[article_idx] = scores[:, 0].detach().numpy()  # 부정 클래스에 대한 스코어
 

### 스코어링 표기
news_text_df[[
    'article_reg_date', 'company_name', 'news_agency',
    'title_positive_score', 'title_negative_score', 'article_positive_score', 'article_negative_score'
]]
news_text_df[[
    #'article_reg_date', 'company_name', 'news_agency',
    'title_positive_score', 'title_negative_score', 'article_positive_score', 'article_negative_score'
]].describe()
# 기업의 일별 score
daily_com_grouped_df  = news_text_df[[
    'article_reg_date', 'company_name',
    'title_positive_score', 'title_negative_score', 'article_positive_score', 'article_negative_score'
]].groupby( ['article_reg_date', 'company_name']).sum()
scores_df = pd.DataFrame()
scores_df['title_index_1'] = daily_com_grouped_df['title_positive_score'] -  daily_com_grouped_df['title_negative_score'] 
scores_df['article_index_1'] = daily_com_grouped_df['article_positive_score'] -  daily_com_grouped_df['article_negative_score']
scores_df



# 설명
news_text_df.columns
#기존의 제공된 정보에서 title의 긍정 부정 이진 분류 score, 긍정 중립 부정 3-class 분류 score가 추가로 생성되어야 함.
# 현재는 binary classification만 존재하는 상황
scores_df.reset_index().columns
# 인덱스를 여러가지 생성 할 수도 있음 ex) title_index_2,3....., article_title_mixed_index.....
#forcast로 news_text_df에 있는 date 범위 이외의 일자 또한 존재가 가능 할 수도 있음
daily_com_grouped_df.reset_index().columns
# 현재는 groupby sum 으로 되어 있지만, 추가적인 가중치를 포함하거나 positive,negative 등의 연산이 바뀔 가능성도 있음.
import dill
from dill import dump, load
dill.dump_session("20240204_1235.dill")
from dill import dump, load
with open('score_dataframes.dill', 'wb') as f:
    dump([news_text_df, scores_df, daily_com_grouped_df], f )
from dill import dump, load
with open('score_dataframes.dill', 'rb') as f:
    news_text_df, scores_df, daily_com_grouped_df = dill.load(f)
"""