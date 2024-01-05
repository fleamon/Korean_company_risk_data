# stock_Korean_by_ESG_BackData

## 개발환경

virtual env

- Python 3.12.1
- pip 23.3.2

## 설치

```
pip install -r requirements.txt
```

## Data Gathering List

## 데이터베이스

데이터베이스 MySQL 설치와 **'stock_Korean_by_ESG_BackData'** DB 생성필요.

```bash
create database stock_Korean_by_ESG_BackData;
```

## 실행

- 하루에 한번가져와야되는 데이터 DailyMain.py
- 일정간격으로 돌아야하는 batch TermBatchMain.py

```bash
python DailyMain.py
python TermBatchMain.py
```
