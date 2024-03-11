import requests
from datetime import datetime, timedelta
import time
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
import re
import CommonFunction as cf


def crawling_articles_from_keyword(query, start_date, end_date, is_public):
    """뉴스기사 DB 저장"""
    # TOBE : Korean_all_code_info 에서 Korean_short_name+ceo_name 이 query로 들어가야함
    # ceo_name 에 대해서 data cleansing 필요함
    # is_public 변수(True, False) 로 상장 / 비상장 구분
    # 상장회사는 Korean_all_code_info 매일 모두 크롤링, 비상장회사는 필요한 회사 받아오도록 수정
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 뉴스기사 DB 저장 !!!!!")
    try:
        maxpage = "1"  # 검색어 넣은 마지막 페이지
        sort = "0"  # 0:관련도순, 1:최신순, 2:오래된순
        print (query)
        start_datetime = datetime.strptime(start_date, "%Y.%m.%d")
        end_datetime = datetime.strptime(end_date, "%Y.%m.%d")
        # 시작 날짜부터 종료 날짜까지 하루씩 감소
        current_datetime = start_datetime
        while current_datetime >= end_datetime:
            crawling_date_id = str(current_datetime.strftime("%Y.%m.%d"))
            current_datetime -= timedelta(days=1)
            # naver_news_crawler(maxpage, query, sort, crawling_date_id, '네이버') 
            daum_news_crawler(maxpage, query, sort, crawling_date_id, '다음') 
            print ("5 seconds sleep...")
            time.sleep(5)

        cf.send_message("KOR", f"{query} 뉴스기사 DB 저장 success!")
        print(f"{query} 뉴스기사 DB 저장 successfully.")
        
    except Exception as e:
        cf.send_message("ERROR", f"뉴스기사 DB 저장 [오류 발생]{e}")
        time.sleep(1)


def naver_news_crawler(maxpage, query, sort, crawling_date_id, portal_name):
    # 각 크롤링 결과 저장하기 위한 리스트 선언 
    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    result={}

    date_id = crawling_date_id.replace(".","")
    page = 1  
    maxpage =(int(maxpage)-1)*10+1  # 11= 2페이지 21=3페이지 31=4페이지  ...81=9페이지 , 91=10페이지, 101=11페이지
    
    while page <= maxpage:
        url = "https://search.naver.com/search.naver?where=news&query=" + query + "&sort="+sort+"&ds=" + crawling_date_id + "&de=" + crawling_date_id + "&nso=so%3Ar%2Cp%3Afrom" + date_id + "to" + date_id + "%2Ca%3A&start=" + str(page)
        print ("url :", url)
        original_html = requests.get(url, headers=cf.headers)
        print ("original_html status : ", original_html)
        html = BeautifulSoup(original_html.text, "html.parser")
        
        url_info = html.select("div.group_news > ul.list_news > li div.news_area > div.news_info > div.info_group > a.info")
        for element in url_info:
            href = element.get("href")
            if "news.naver.com" in href:
                article_link.append(href)
        
        page = page + 10
    
    to_remove_url = []
    
    for naver_url in article_link:
        naver_original_html = requests.get(naver_url, headers=cf.headers)
        print ("naver_original_html status : ", naver_original_html)
        naver_html = BeautifulSoup(naver_original_html.text, "html.parser")

        # 기사 발행일
        html_date = naver_html.select_one("div#ct> div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div > span")
        if html_date is None:
            to_remove_url.append(naver_url)
            continue
        news_date = html_date.attrs['data-date-time']
        article_reg_date.append(news_date)

        # 언론사
        news_agency.append( naver_html.select_one('meta:nth-child(10)').get("content") )

        # 뉴스 제목
        title = naver_html.select_one("#ct > div.media_end_head.go_trans > div.media_end_head_title > h2")
        if title == None:
            title = naver_html.select_one("#content > div.end_ct > div > h2")

        # 뉴스 본문
        article = naver_html.select("article#dic_area")
        if article == []:
            article = naver_html.select("#articeBody")
        article = ''.join(str(article))
        
        # html태그제거 및 텍스트 다듬기
        pattern1 = '<[^>]*>'
        pattern2 = '"'
        pattern3 = "'"
        pattern4 = '\t'

        title = re.sub(pattern=pattern1, repl='', string=str(title))
        title = re.sub(pattern=pattern2, repl='', string=str(title))
        title = re.sub(pattern=pattern3, repl='', string=str(title))
        title = re.sub(pattern=pattern4, repl='', string=str(title))
        
        article = re.sub(pattern=pattern1, repl='', string=article)
        article = re.sub(pattern=pattern2, repl='', string=article)
        article = re.sub(pattern=pattern3, repl='', string=article)
        article = re.sub(pattern=pattern4, repl='', string=article)
        
        titles.append(title)
        article_text.append(article)
        
    for remove_url in to_remove_url:
        article_link.remove(remove_url)

    result= {
            "article_reg_date" : article_reg_date
            , "article_link": article_link
            , "company_name": query.split("+")[0]
            , "news_agency" : news_agency
            , "titles" : titles
            , "article_text": article_text
            }
    df = pd.DataFrame(result)  # df로 변환
    df = df.drop_duplicates()

    # # 새로 만들 파일이름 지정
    # now = datetime.now()  # 파일이름 현 시간으로 저장하기
    # outputFileName = f'articles_from_naver_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}_{now.second}.xlsx'
    # df.to_excel('/Users/kakao/Downloads/'+outputFileName,sheet_name='sheet1')
    
    # DB에 delete - insert
    conn = cf.connect_to_db()
    cursor = conn.cursor()

    delete_query = f'''
        DELETE FROM stock_Korean_by_ESG_BackData.articles
            WHERE article_reg_date = '{crawling_date_id.replace(".","-")}'
            AND search_keyword = '{query}'
            AND portal_name = '{portal_name}'
            AND company_name = '{query.split("+")[0]}'
    '''
    cursor.execute(delete_query)

    for index, row in df.iterrows():
        insert_query = f'''
            INSERT INTO stock_Korean_by_ESG_BackData.articles 
            (article_reg_date, portal_name, article_link, company_name, news_agency, search_keyword, title, article_text, load_date)
            VALUES 
            ('{row["article_reg_date"]}', '{portal_name}', '{row["article_link"]}'
            , '{row["company_name"]}', '{row["news_agency"]}', '{query}'
            , '{row["titles"]}', '{row["article_text"]}', NOW())
            ON DUPLICATE KEY UPDATE 
            article_link=VALUES(article_link), 
            title=VALUES(title), 
            article_text=VALUES(article_text)
        '''
        cursor.execute(insert_query)
        
    conn.commit()
    cursor.close()
    conn.close()


def daum_news_crawler(maxpage, query, sort, crawling_date_id, portal_name):
    # 각 크롤링 결과 저장하기 위한 리스트 선언 
    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    result={}

    date_id = crawling_date_id.replace(".","")
    page = 1  
    maxpage =(int(maxpage)-1)*10+1  # 11= 2페이지 21=3페이지 31=4페이지  ...81=9페이지 , 91=10페이지, 101=11페이지
    
    while page <= maxpage:
        url = "https://search.naver.com/search.naver?where=news&query=" + query + "&sort="+sort+"&ds=" + crawling_date_id + "&de=" + crawling_date_id + "&nso=so%3Ar%2Cp%3Afrom" + date_id + "to" + date_id + "%2Ca%3A&start=" + str(page)
        print ("url :", url)
        original_html = requests.get(url, headers=cf.headers)
        print ("original_html status : ", original_html)
        html = BeautifulSoup(original_html.text, "html.parser")
        
        url_info = html.select("div.group_news > ul.list_news > li div.news_area > div.news_info > div.info_group > a.info")
        for element in url_info:
            href = element.get("href")
            if "news.naver.com" in href:
                article_link.append(href)
        
        page = page + 10
    
    to_remove_url = []
    
    for naver_url in article_link:
        naver_original_html = requests.get(naver_url, headers=cf.headers)
        print ("naver_original_html status : ", naver_original_html)
        naver_html = BeautifulSoup(naver_original_html.text, "html.parser")

        # 기사 발행일
        html_date = naver_html.select_one("div#ct> div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > div > span")
        if html_date is None:
            to_remove_url.append(naver_url)
            continue
        news_date = html_date.attrs['data-date-time']
        article_reg_date.append(news_date)

        # 언론사
        news_agency.append( naver_html.select_one('meta:nth-child(10)').get("content") )

        # 뉴스 제목
        title = naver_html.select_one("#ct > div.media_end_head.go_trans > div.media_end_head_title > h2")
        if title == None:
            title = naver_html.select_one("#content > div.end_ct > div > h2")

        # 뉴스 본문
        article = naver_html.select("article#dic_area")
        if article == []:
            article = naver_html.select("#articeBody")
        article = ''.join(str(article))
        
        # html태그제거 및 텍스트 다듬기
        pattern1 = '<[^>]*>'
        pattern2 = '"'
        pattern3 = "'"
        pattern4 = '\t'

        title = re.sub(pattern=pattern1, repl='', string=str(title))
        title = re.sub(pattern=pattern2, repl='', string=str(title))
        title = re.sub(pattern=pattern3, repl='', string=str(title))
        title = re.sub(pattern=pattern4, repl='', string=str(title))
        
        article = re.sub(pattern=pattern1, repl='', string=article)
        article = re.sub(pattern=pattern2, repl='', string=article)
        article = re.sub(pattern=pattern3, repl='', string=article)
        article = re.sub(pattern=pattern4, repl='', string=article)
        
        titles.append(title)
        article_text.append(article)
        
    for remove_url in to_remove_url:
        article_link.remove(remove_url)

    result= {
            "article_reg_date" : article_reg_date
            , "article_link": article_link
            , "company_name": query.split("+")[0]
            , "news_agency" : news_agency
            , "titles" : titles
            , "article_text": article_text
            }
    df = pd.DataFrame(result)  # df로 변환
    df = df.drop_duplicates()

    # # 새로 만들 파일이름 지정
    # now = datetime.now()  # 파일이름 현 시간으로 저장하기
    # outputFileName = f'articles_from_naver_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}_{now.second}.xlsx'
    # df.to_excel('/Users/kakao/Downloads/'+outputFileName,sheet_name='sheet1')
    
    # DB에 delete - insert
    conn = cf.connect_to_db()
    cursor = conn.cursor()

    delete_query = f'''
        DELETE FROM stock_Korean_by_ESG_BackData.articles
            WHERE article_reg_date = '{crawling_date_id.replace(".","-")}'
            AND search_keyword = '{query}'
            AND portal_name = '{portal_name}'
            AND company_name = '{query.split("+")[0]}'
    '''
    cursor.execute(delete_query)

    for index, row in df.iterrows():
        insert_query = f'''
            INSERT INTO stock_Korean_by_ESG_BackData.articles 
            (article_reg_date, portal_name, article_link, company_name, news_agency, search_keyword, title, article_text, load_date)
            VALUES 
            ('{row["article_reg_date"]}', '{portal_name}', '{row["article_link"]}'
            , '{row["company_name"]}', '{row["news_agency"]}', '{query}'
            , '{row["titles"]}', '{row["article_text"]}', NOW())
            ON DUPLICATE KEY UPDATE 
            article_link=VALUES(article_link), 
            title=VALUES(title), 
            article_text=VALUES(article_text)
        '''
        cursor.execute(insert_query)
        
    conn.commit()
    cursor.close()
    conn.close()