import requests
from datetime import datetime, timedelta
import time
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
import re
import CommonFunction as cf


# html태그제거 및 텍스트 다듬기
pattern1 = '<[^>]*>'
pattern2 = '"'
pattern3 = "'"
pattern4 = '\t'
pattern5 = r'[^\w\s]'  # 이모지 패턴
patterns = [pattern1, pattern2, pattern3, pattern4, pattern5]


def crawling_articles_from_keyword(query, start_date, end_date, is_public):
    """뉴스기사 DB 저장"""
    # TOBE : Korean_all_code_info 에서 Korean_short_name+ceo_name 이 query로 들어가야함
    # ceo_name 에 대해서 data cleansing 필요함
    # is_public 변수(True, False) 로 상장 / 비상장 구분
    # 상장회사는 Korean_all_code_info 매일 모두 크롤링, 비상장회사는 필요한 회사 받아오도록 수정
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 뉴스기사 DB 저장 !!!!!")
    try:
        maxpage = "1"  # 검색어 넣은 마지막 페이지
        naver_sort = "0"  # 0:관련도순, 1:최신순, 2:오래된순
        daum_sort = "accuracy"  # accuracy:정확도순, recency:최신순, old:오래된순
        print (query)
        start_datetime = datetime.strptime(start_date, "%Y.%m.%d")
        end_datetime = datetime.strptime(end_date, "%Y.%m.%d")
        # 시작 날짜부터 종료 날짜까지 하루씩 감소
        current_datetime = start_datetime
        while current_datetime >= end_datetime:
            crawling_date_id = str(current_datetime.strftime("%Y.%m.%d"))
            current_datetime -= timedelta(days=1)
            naver_news_crawler(maxpage, query, naver_sort, crawling_date_id, '네이버') 
            daum_news_crawler(maxpage, query, daum_sort, crawling_date_id, '다음') 
            print ("5 seconds sleep...")
            time.sleep(5)

        cf.send_message("KOR", f"{query} 뉴스기사 DB 저장 success!")
        print(f"{query} 뉴스기사 DB 저장 successfully.")
        
    except Exception as e:
        cf.send_message("ERROR", f"뉴스기사 DB 저장 [오류 발생]{e}")
        time.sleep(1)


def naver_news_crawler(maxpage, query, naver_sort, crawling_date_id, portal_name):
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
        url = "https://search.naver.com/search.naver?where=news&query=" + query + "&sort="+naver_sort+"&ds=" + crawling_date_id + "&de=" + crawling_date_id + "&nso=so%3Ar%2Cp%3Afrom" + date_id + "to" + date_id + "%2Ca%3A&start=" + str(page)
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
        
        for pattern in patterns:
            title = re.sub(pattern=pattern, repl='', string=str(title))

        for pattern in patterns:
            article = re.sub(pattern=pattern, repl='', string=str(article))
        
        titles.append(title)
        article_text.append(article)
        
    for remove_url in to_remove_url:
        article_link.remove(remove_url)

    cf.result_delete_insert_to_db_articles_table(query, article_reg_date, article_link, news_agency, titles, article_text, crawling_date_id, portal_name)


def daum_news_crawler(maxpage, query, daum_sort, crawling_date_id, portal_name):
    # 각 크롤링 결과 저장하기 위한 리스트 선언 
    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    result={}
    date_id = crawling_date_id.replace(".","")
    page = 1
    
    while page <= int(maxpage):
        url = 'https://search.daum.net/search?w=news&nil_search=btn&DA=PGD&enc=utf8&cluster=y&cluster_page=1&q=' + query + '&sd=' + date_id + '000000&ed=' + date_id + '235959&period=u&p=' + str(page) + '&sort=' + daum_sort
        print ("url :", url)
        original_html = requests.get(url, headers=cf.headers)
        page = page + 1
        print ("original_html status : ", original_html)
        html = BeautifulSoup(original_html.text, "html.parser")
        # ul 태그 중 클래스가 "grid_xscroll"인 면 제거
        grid_xscroll_elements = html.find_all("ul", class_="grid_xscroll")
        for element in grid_xscroll_elements:
            element.decompose()
        # 기사 관련 정보 크롤링
        # "strong" 태그의 "class" 속성이 "tit_item"인 요소 추출
        strong_tit_item = html.find_all("strong", class_="tit_item")
        for title in strong_tit_item:
            if title.get('title'):
                news_agency.append(title.text.strip())
        # "div" 태그의 "class" 속성이 "item-title"인 요소 추출
        div_item_title = html.find_all("div", class_="item-title")
        for article_title in div_item_title:
            title = article_title.text.strip()
            for pattern in patterns:
                title = re.sub(pattern=pattern, repl='', string=str(title))
            titles.append(title)
            article_link.append(article_title.find("a")['href'])
            # 기사 내용 크롤링
            daum_original_html = requests.get(article_title.find("a")['href'], headers=cf.headers)
            print ("daum_original_html status : ", daum_original_html)
            daum_html = BeautifulSoup(daum_original_html.text, "html.parser")
            # "span" 태그의 "class" 속성이 "gem-subinfo"인 요소 추출
            div_article_view = daum_html.find("div", class_="article_view")
            # print (div_article_view)
            paragraphs = div_article_view.find_all('p')
            article = ''
            for p in paragraphs:
                article = article + p.get_text().strip()
            for pattern in patterns:
                article = re.sub(pattern=pattern, repl='', string=str(article))
            article_text.append(article)
        # "span" 태그의 "class" 속성이 "gem-subinfo"인 요소 추출
        div_item_contents = html.find_all("span", class_="gem-subinfo")
        for article_date in div_item_contents:
            tmp_article_date_arr = article_date.text.strip().split()
            if len(tmp_article_date_arr) > 1:
                tmp_article_date = tmp_article_date_arr[0]
            else:
                tmp_article_date = article_date.text.strip()
            article_reg_date.append(tmp_article_date.replace(".", "-"))

    cf.result_delete_insert_to_db_articles_table(query, article_reg_date, article_link, news_agency, titles, article_text, crawling_date_id, portal_name)
