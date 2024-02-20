import dill
import re
import pandas as pd
from IPython.display import display


def main():
    with open('./dill_files/20240215_0038_score_dataframes.dill', 'rb') as f:
        data = dill.load(f)

    print ()
    print (data)
    print (data.columns)
    data_column_names = ', '.join(data.columns)
    print("데이터프레임의 컬럼: ", data_column_names)
    print ()
    ############################################################
    ############################################################

    esg_keyword_list = ['기업','ESG','사회','경영','책임',
                        '가치','투자','지배구조','환경','CSR',
                        '성과','재무','정보','평가','지속가능',
                        '등급','공시','효과','고려','시장',
                        '요소','활용','전략','지속','관리',
                        '가능성','변화','위험','회사','경영자',]

    ############################################################

    e_positive_keyword_list = ['친환경', '탄소중립', '생물 다양성',
                            '에너지 효율', 'RE100', '생태계',
                            '생물 다양성', '전환', '체결',
                            '수소', '저탄소', '실천','친환경 소비',
                            ]

    e_negative_keyword_list = ['그린워싱', '탄소 배출', '배출', 
                            '플라스틱', '인체', '폐기물', 
                            '기후변화', '재난', '오염', 
                            '지구 온난화', '환경 파괴', '미세먼지', '종이컵',
                            ]

    ############################################################

    s_positive_keyword_list = ['상생', '지역사회', '협력', '사회적 책임',
                            '고객', '고객 만족', '공급망 관리', 
                            '근로자 안전', '프라이버시', '데이터 보호', 
                            '노조', '사회 환원', '일자리',]

    s_negative_keyword_list  = ['기술 탈취', '독점', '불공정 경쟁', 
                                '이중계약', '문어발', '해고', 
                                '불법', '척결', '처벌', '형사처벌', 
                                '반대 단체', '청탁', '부정 청탁',]

    ############################################################

    g_positive_keyword_list =  ['주주권', '주주 보호', '사외이사', 
                                '다양성', '주주 환원', '윤리 경영', 
                                '책임 경영', '성장', '글로벌', 
                                '평가', '투자', '미래', '윤리',]

    g_negative_keyword_list = ['구속', '법정구속', '압수수색', 
                            '사법 리스크', '조작', '인수 무산', 
                            '실형', '뇌물', '시세조종', '기소', 
                            '위반', '재판', '리스크',]

    ############################################################

    keword_count_column_list = []
    keword_isin_column_list = []
    #for keyword in esg_keyword_list :
    for keyword in e_positive_keyword_list + e_negative_keyword_list + s_positive_keyword_list + s_negative_keyword_list + g_positive_keyword_list + g_negative_keyword_list :
        column_name = 'keword_'+ keyword +'_count'
        data[column_name] = data['article_text'].str.count( keyword )
        keword_count_column_list.append(column_name)

        column_name = 'keword_'+ keyword +'_isin'
        data[column_name] = data['article_text'].str.count( keyword )
        keword_isin_column_list.append(column_name)

    data['esg_cnt_weight'] = data[keword_count_column_list].sum(axis = 1)
    data['esg_isin_weight'] = data[keword_isin_column_list].sum(axis = 1)

    data['esg_cnt_article_positive_score'] = data['article_positive_score'] * data['esg_cnt_weight'] 
    data['esg_cnt_article_negative_score'] = data['article_negative_score'] * data['esg_cnt_weight'] 

    data['esg_isin_article_positive_score'] = data['article_positive_score'] * data['esg_isin_weight'] 
    data['esg_isin_article_negative_score'] = data['article_negative_score'] * data['esg_isin_weight'] 

    data["title_class"] = data[['title_positive_score', 'title_neutral_score', 'title_negative_score']].idxmax(axis = 1).str.replace("title_", "").str.replace("article_", "").str.replace("_score", "")
    data["article_class"] =  data[['article_positive_score', 'article_neutral_score', 'article_negative_score']].idxmax(axis = 1).str.replace("title_", "").str.replace("article_", "").str.replace("_score", "")

    print ()
    print (data)
    print (data.columns)
    data_column_names = ', '.join(data.columns)
    print("데이터프레임의 컬럼: ", data_column_names)
    print ()

    # compare_targets_dict = {
    #     '삼성전자' : ['현대자동차', 'LG', 'SK'],
    #     '카카오' : ['네이버', '라인', '쿠팡', '우아한형제들', '배달의민족'],
    #     'VCNC' : ['카카오모빌리티', '우버코리아'],
    #     '토스' : ['카카오페이', '네이버파이낸셜'],
    # }

    # company_main_event = { 
    #     '삼성전자' : [
    #         '2020-06-04', # 6월 4일 = 검찰, 이재용 회장 등 3명 주식시세 조종·분식회계 혐의 구속영장 청구
    #         '2020-06-09', # 6월 9일 = 이재용 회장 등 3명 구속영장 기각
    #         '2020-09-01', # 9월 1일 = 서울중앙지검, '삼성 부당 합병·승계 의혹' 이 회장 등 11명 불구속 기소
    #         '2021-01-18', # 1월 18일 = 이재용 부회장 구속
    #     ],
        
    #     '카카오' : [],
        
    #     'VCNC' : [
    #         '2019-07-17', # 2019년 7월 17일 / 국토부, '혁신성장과 상생발전을 위한 택시 제도 개편 방안' 발표 (타다 금지)
    #         '2019-10-28', # 2019년 10월 28일 / 검찰, 타다(박재욱, 이재웅(쏘카 당시 대표)) 기소
    #         '2020-02-19', # 2020년 2월 19일 / 중앙지법, 타다 무죄 판결
    #         '2020-05-01', # 2020년 5월 1일 / 타다, 타다금지법에 대한 헌법소원 제기
    #         '2020-06-24', # 2020년 5월 1일 / 타다, 타다금지법에 대한 헌법소원 제기
    #     ],
        
    #     '토스' : ['2022-10-01', '2023-04-01'], 
    #     #*일단 기사를 찾아보니 최근 부정적인 이슈가
    #     #(1) 토스 이용자 개인 정보 판매 의혹 건(2022년 10월 국감)
    #     #(2) 토스뱅크 유동성 위기(2023년 4월 전후)
    #     # 정확하지 않은 일자.
    # }
    
    # for target_company_name in compare_targets_dict.keys():
    #     print ("회사이름 : ", target_company_name)
    
    #     compare_targets_list = compare_targets_dict[target_company_name]
        
    #     # 일별 점수 산출
    #     daily_grouped_scroes_df = data.query( " company_name == @target_company_name or company_name in @compare_targets_list")[
    #         [
    #             'article_reg_date', 'company_name',
                
    #             'title_positive_score', 'title_negative_score', 
    #             'article_positive_score', 'article_negative_score',

    #             #'keword_기업_count', 'keword_ESG_count', 'keword_사회_count', 'keword_경영_count',
    #             #'keword_책임_count', 'keword_가치_count', 'keword_투자_count', 'keword_지배구조_count',
    #             #'keword_환경_count', 'keword_CSR_count', 'keword_성과_count', 'keword_재무_count',
    #             #'keword_정보_count', 'keword_평가_count', 'keword_지속가능_count', 'keword_등급_count',
    #             #'keword_공시_count', 'keword_효과_count', 'keword_고려_count', 'keword_시장_count',
    #             #'keword_요소_count', 'keword_활용_count', 'keword_전략_count', 'keword_지속_count',
    #             #'keword_관리_count', 'keword_가능성_count', 'keword_변화_count', 'keword_위험_count',
    #             #'keword_회사_count', 'keword_경영자_count',


    #             'keword_친환경_count', 'keword_탄소중립_count', 'keword_생물 다양성_count', 
    #             'keword_에너지 효율_count', 'keword_RE100_count', 'keword_생태계_count', 
    #             'keword_생물 다양성_count', 'keword_전환_count', 'keword_체결_count',
    #             'keword_수소_count', 'keword_저탄소_count', 'keword_실천_count',
    #             'keword_친환경 소비_count', 

    #             #
                
    #             'keword_그린워싱_count', 'keword_탄소 배출_count', 'keword_배출_count',
    #             'keword_플라스틱_count', 'keword_인체_count', 'keword_폐기물_count', 
    #             'keword_기후변화_count', 'keword_재난_count', 'keword_오염_count',
    #             'keword_지구 온난화_count', 'keword_환경 파괴_count', 'keword_미세먼지_count', 
    #             'keword_종이컵_count', 

    #             ##
                
    #             'keword_상생_count', 'keword_지역사회_count', 'keword_협력_count', 
    #             'keword_사회적 책임_count', 'keword_고객_count', 'keword_고객 만족_count', 
    #             'keword_공급망 관리_count', 'keword_근로자 안전_count', 'keword_프라이버시_count', 
    #             'keword_데이터 보호_count', 'keword_노조_count', 'keword_사회 환원_count', 
    #             'keword_일자리_count', 

    #             #
                
    #             'keword_기술 탈취_count', 'keword_독점_count', 'keword_불공정 경쟁_count', 
    #             'keword_이중계약_count', 'keword_문어발_count', 'keword_해고_count',
    #             'keword_불법_count', 'keword_척결_count', 'keword_처벌_count',
    #             'keword_형사처벌_count', 'keword_반대 단체_count', 'keword_청탁_count',
    #             'keword_부정 청탁_count', 
                
    #             ##
                
    #             'keword_주주권_count', 'keword_주주 보호_count', 'keword_사외이사_count', 
    #             'keword_다양성_count', 'keword_주주 환원_count', 'keword_윤리 경영_count', 
    #             'keword_책임 경영_count', 'keword_성장_count', 'keword_글로벌_count', 
    #             'keword_평가_count', 'keword_투자_count', 'keword_미래_count',
    #             'keword_윤리_count',

    #             #
                
    #             'keword_구속_count', 'keword_법정구속_count', 'keword_압수수색_count', 
    #             'keword_사법 리스크_count', 'keword_조작_count','keword_인수 무산_count', 
    #             'keword_실형_count', 'keword_뇌물_count', 'keword_시세조종_count', 
    #             'keword_기소_count', 'keword_위반_count', 'keword_재판_count',
    #             'keword_리스크_count',
                
    #             'esg_cnt_weight',

    #             #'title_class', 
    #             #'article_class',
    #         ]
    #     ].groupby( ['article_reg_date', 'company_name']).agg( { 'sum', 'count' }).reset_index().set_index([ 'article_reg_date',  'company_name'] )

    #     # 데이터 프레임 생성
    #     daily_grouped_scroes_df = pd.pivot(daily_grouped_scroes_df.reset_index() , index='article_reg_date', columns='company_name').asfreq('D').fillna(0)
    #     print ()
    #     print (daily_grouped_scroes_df)
    #     # daily_grouped_scroes_df_column = ', '.join(daily_grouped_scroes_df.columns)
    #     print (daily_grouped_scroes_df.columns)
    #     print ()

    #     # 일별 스코어 최종 집계
    #     article_scores_df = (daily_grouped_scroes_df['article_positive_score']['sum']  * daily_grouped_scroes_df['article_positive_score']['count'] -  daily_grouped_scroes_df['article_negative_score']['sum']   * daily_grouped_scroes_df['article_negative_score']['count']).rolling(5).mean()
    #     # print ()
    #     print (article_scores_df)
    #     # print ()
    #     article_scores_df_column = ', '.join(article_scores_df.columns)
    #     print ("chk!!!!")
    #     print (article_scores_df_column)

        
    #     import plotly.express as px

    #     color_map_dict = {}
    #     color_map_dict[target_company_name] = "red"
    #     for comp_name in compare_targets_list :
    #         color_map_dict[comp_name] = "black"    

    #     fig = px.line( 
    #         pd.melt(
    #             article_scores_df.reset_index(), 
    #             id_vars = ['article_reg_date'],
    #         ),
    #         x="article_reg_date", 
    #         y="value", 
    #         color = "company_name",
    #         title='기사 감성 score [' + target_company_name + ']',
    #         color_discrete_map = color_map_dict
    #                 )
    #     # 나중에 이슈 관련이 전부 생성된다면, 반복문 돌려서 해당 일자 수직선 생성할것.
    #     for date in company_main_event[target_company_name]:
    #         fig.add_vline(x=date, line_width=3, line_dash="dash")
    #     # fig.show()

    #     keyword_count_df = daily_grouped_scroes_df.xs(
    #         key= target_company_name ,
    #         level=2, 
    #         axis=1).xs(
    #             key= 'sum',
    #             level=1,
    #             axis=1)[ keword_count_column_list + ['esg_cnt_weight'] ]


    #     keyword_count_df.columns = [re.sub(r'keword_|_count', '', s) for s in keyword_count_df.columns.tolist() ]
    #     # print ()
    #     # print (keyword_count_df.columns)
    #     # print ()

    #     import plotly.express as px

    #     fig = px.line(
    #         pd.melt(keyword_count_df.reset_index(), 
    #                 id_vars= ['article_reg_date']
    #             ),
    #         x="article_reg_date", 
    #         y="value", 
    #         color = "variable", 
    #         title='기사 esg 키워드 등장 빈도 [' + target_company_name + ']',
    #     )
    #     for date in company_main_event[target_company_name]:
    #         fig.add_vline(x=date, line_width=3, line_dash="dash")
    #     # fig.show()