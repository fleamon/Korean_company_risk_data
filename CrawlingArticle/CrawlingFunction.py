import requests
from datetime import datetime, timedelta
import time
from bs4 import BeautifulSoup
import CommonFunction as cf


def crawling_articles_from_keyword(query, start_date, end_date, is_public):
    """뉴스기사 DB 저장"""
    # TOBE : Korean_all_code_info 에서 Korean_short_name+ceo_name 이 query로 들어가야함
    # ceo_name 에 대해서 data cleansing 필요함
    # is_public 변수(True, False) 로 상장 / 비상장 구분
    # 상장회사는 Korean_all_code_info 매일 모두 크롤링, 비상장회사는 필요한 회사 받아오도록 수정
    print([time.strftime('%Y-%m-%d %H:%M:%S')], "!!!!! DEBUG PRINT : 뉴스기사 DB 저장 !!!!!")
    try:
        dummypage = 1  # 사용하지 않는 변수
        dummysort = 1  # 사용하지 않는 변수
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
            # naver_news_crawler(maxpage, query, naver_sort, crawling_date_id, '네이버') 
            # daum_news_crawler(maxpage, query, daum_sort, crawling_date_id, '다음') 
            # dcinside_articles_crawler(dummypage, query, dummysort, crawling_date_id, '디시인사이드') 
            fmkorea_articles_crawler(dummypage, query, dummysort, crawling_date_id, '에펨코리아') 
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

        # 언론사
        if naver_html.select_one('meta:nth-child(10)') is None:
            to_remove_url.append(naver_url)
            continue
        news_agency.append(naver_html.select_one('meta:nth-child(10)').get("content"))

        # 기사 발행일
        article_reg_date.append(crawling_date_id.replace(".","-"))

        # 뉴스 제목
        title = naver_html.select_one("#ct > div.media_end_head.go_trans > div.media_end_head_title > h2")
        if title == None:
            title = naver_html.select_one("#content > div.end_ct > div > h2")

        # 뉴스 본문
        article = naver_html.select("article#dic_area")
        if article == []:
            article = naver_html.select("#articeBody")
        article = ''.join(str(article))
        
        title = cf.delete_patterns(title)
        article = cf.delete_patterns(article)
        
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
        # "div" 태그의 "class" 속성이 "item-title"인 요소 추출 (link 추출)
        div_item_title = html.find_all("div", class_="item-title")
        for article_title in div_item_title:
            # 기사 내용 크롤링
            daum_original_html = requests.get(article_title.find("a")['href'], headers=cf.headers)
            if daum_original_html.status_code != 200:
                continue
            print ("daum_original_html status : ", daum_original_html)
            title = cf.delete_patterns(article_title.text.strip())
            titles.append(title)  # 기사제목 추출
            article_link.append(article_title.find("a")['href'])  # 기사링크 추출
            daum_html = BeautifulSoup(daum_original_html.text, "html.parser")
            # "h1" 태그의 "class" 속성이 "doc-title"인 요소 추출 (언론사 추출)
            h1_class = daum_html.find("h1", class_="doc-title")
            news_agency.append(h1_class.get_text().strip())
            
            # "span" 태그의 "class" 속성이 "gem-subinfo"인 요소 추출 (기사내용 추출)
            div_article_view = daum_html.find("div", class_="article_view")
            paragraphs = div_article_view.find_all('p')
            article = ''
            for p in paragraphs:
                article = article + p.get_text().strip()
            article = cf.delete_patterns(article)
            article_text.append(article)
            article_reg_date.append(crawling_date_id.replace(".", "-"))

    cf.result_delete_insert_to_db_articles_table(query, article_reg_date, article_link, news_agency, titles, article_text, crawling_date_id, portal_name)


def dcinside_articles_crawler(dummypage, query, dummysort, crawling_date_id, portal_name):
    # 각 크롤링 결과 저장하기 위한 리스트 선언 
    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    
    if '+' in query:
        func_query = query.replace('+', ' ')
    url = 'https://search.dcinside.com/combine/q/' + func_query
    print ("url :", url)
    original_html = requests.get(url, headers=cf.headers)
    print ("original_html status : ", original_html)
    html = BeautifulSoup(original_html.text, "html.parser")
    # dcinside news
    ul_list = html.find_all("ul", class_="sch_result_list")
    li_tags = ul_list[0].find_all('li')
    for li_tag in li_tags:
        date_time = li_tag.find('span', class_='date_time').text
        if date_time.split(' ')[0] != crawling_date_id:
            continue
        else:
            article_url = requests.get(li_tag.find("a")['href'], headers=cf.headers)
            article_html = BeautifulSoup(article_url.text, "html.parser")
            article_reg_date.append(crawling_date_id)
            article_link.append(li_tag.find("a")['href'])
            article_title = cf.delete_patterns(article_html.find('h1', class_='headline mg').text)
            titles.append(article_title)
            article_html_agency = article_html.find('div', class_='byline').find('em', class_='mg')
            news_agency.append(article_html_agency.text[1:-1])
            article_content = cf.delete_patterns(article_html.find('div', class_='article_body fs1 mg').text)
            article_text.append(article_content)
            
    cf.result_delete_insert_to_db_articles_table(query, article_reg_date, article_link, news_agency, titles, article_text, crawling_date_id, portal_name)

    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    
    # dcinside post
    li_tags = ul_list[1].find_all('li')
    for li_tag in li_tags:
        date_time = li_tag.find('span', class_='date_time').text
        if date_time.split(' ')[0] != crawling_date_id:
            continue
        else:
            article_url = requests.get(li_tag.find("a")['href'], headers=cf.headers)
            article_html = BeautifulSoup(article_url.text, "html.parser")
            article_reg_date.append(crawling_date_id)
            article_link.append(li_tag.find("a")['href'])
            article_title = cf.delete_patterns(article_html.find('span', class_='title_subject').text.strip())
            titles.append(article_title)
            article_html_agency = article_html.find('div', class_='fl clear').text.strip()
            news_agency.append(article_html_agency)
            article_content = cf.delete_patterns(article_html.find('div', class_='write_div').text.strip())
            article_text.append(article_content)
            
    cf.result_delete_insert_to_db_articles_table(query, article_reg_date, article_link, news_agency, titles, article_text, crawling_date_id, portal_name)


def fmkorea_articles_crawler(dummypage, query, dummysort, crawling_date_id, portal_name):
    # 각 크롤링 결과 저장하기 위한 리스트 선언 
    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    
    if '+' in query:
        func_query = query.replace('+', ' ')
    url = 'https://search.dcinside.com/combine/q/' + func_query
    print ("url :", url)
    original_html = requests.get(url, headers=cf.headers)
    print ("original_html status : ", original_html)
    html = BeautifulSoup(original_html.text, "html.parser")
    # dcinside news
    ul_list = html.find_all("ul", class_="sch_result_list")
    li_tags = ul_list[0].find_all('li')
    for li_tag in li_tags:
        date_time = li_tag.find('span', class_='date_time').text
        if date_time.split(' ')[0] != crawling_date_id:
            continue
        else:
            article_url = requests.get(li_tag.find("a")['href'], headers=cf.headers)
            article_html = BeautifulSoup(article_url.text, "html.parser")
            article_reg_date.append(crawling_date_id)
            article_link.append(li_tag.find("a")['href'])
            article_title = cf.delete_patterns(article_html.find('h1', class_='headline mg').text)
            titles.append(article_title)
            article_html_agency = article_html.find('div', class_='byline').find('em', class_='mg')
            news_agency.append(article_html_agency.text[1:-1])
            article_content = cf.delete_patterns(article_html.find('div', class_='article_body fs1 mg').text)
            article_text.append(article_content)
            
    cf.result_delete_insert_to_db_articles_table(query, article_reg_date, article_link, news_agency, titles, article_text, crawling_date_id, portal_name)

    article_reg_date=[]
    article_link=[]
    news_agency=[]
    titles=[]
    article_text=[]
    
    # dcinside post
    li_tags = ul_list[1].find_all('li')
    for li_tag in li_tags:
        date_time = li_tag.find('span', class_='date_time').text
        if date_time.split(' ')[0] != crawling_date_id:
            continue
        else:
            article_url = requests.get(li_tag.find("a")['href'], headers=cf.headers)
            article_html = BeautifulSoup(article_url.text, "html.parser")
            article_reg_date.append(crawling_date_id)
            article_link.append(li_tag.find("a")['href'])
            article_title = cf.delete_patterns(article_html.find('span', class_='title_subject').text.strip())
            titles.append(article_title)
            article_html_agency = article_html.find('div', class_='fl clear').text.strip()
            news_agency.append(article_html_agency)
            article_content = cf.delete_patterns(article_html.find('div', class_='write_div').text.strip())
            article_text.append(article_content)
            
    cf.result_delete_insert_to_db_articles_table(query, article_reg_date, article_link, news_agency, titles, article_text, crawling_date_id, portal_name)
