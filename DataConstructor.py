import csv
from pprint import pprint
from time import sleep
import os
from os.path import isfile, join
from os import listdir

import pymongo
from bs4 import BeautifulSoup
import requests

from StockDB import stockDB
from pymongo import UpdateOne
from pymongo import InsertOne

import selenium
from selenium import webdriver
from selenium.webdriver import ActionChains

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

#from datetime import datetime
import datetime as dt
import time
import re

import pandas as pd
import numpy as np

import logging

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s - %(filename)s:%(lineno)d',
    level=logging.INFO)


# 데이터 베이스 구축 관련 기능

class DatabaseConstructor:

    # 오늘의 전종목 시세 파일 다운로드 후 DB 업데이트
    def updateTodayStockPriceInfo(self): #date 포멧ex) 20210327
        self.__downloadTodayStockPriceInfoFile()

        # 다운로드 폴더에서 다운된 전종목 시세 파일 경로 확인
        downloadPath = 'C:\\Users\\cerxy\\Downloads'

        files = [join(downloadPath, f) for f in listdir(downloadPath) if isfile(join(downloadPath, f)) and ".csv" in f]

        today = dt.datetime.today().strftime('%Y%m%d')
        
        # 현재 가격 정보 업데이트
        for filepath in files:
            if today in filepath:
                self.__update_stock_corp_data(filepath, today)
                break

        print('종목 현재가 업데이트 완료!')

    # KRX 정보시스템에서 오늘의 전정목 시세 정보 파일 다운로드
    def __downloadTodayStockPriceInfoFile(self, dateList=None):
        options = webdriver.ChromeOptions()
        #options.add_argument('headless')
        #options.add_argument('disable-gpu')
        options.add_argument('window-size=1920,1080')
        driver = webdriver.Chrome(executable_path='./resources/webdriver/chromedriver', options=options)
        driver.implicitly_wait(2)
        driver.get(url='http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101')   #KRX 정보데이터 시스템 / 전종목시세
        sleep(3)

        # try:
        #     # EC: expected_conditions
        #     element = WebDriverWait(self.driver, 5).until(
        #         EC.presence_of_element_located((By.CLASS_NAME, 'CI-GRID-ODD')) #웹페이지에서 class가 CI-GRID-ODD인 어떤 element를 찾을 수 있는지를 최대 5초 동안 매 0.5초마다 시도한다.
        #     )
        # finally:
        #     self.driver.quit()

        dataDownload_btn = driver.find_element_by_xpath(
            '//*[@id="MDCSTAT015_FORM"]/div[2]/div/p[2]/button[2]/img')  # 시세정보 다운로드 버튼

        if dateList != None:
            downloadPath = 'C:\\Users\\박혜미\\Downloads'
            datepicker = driver.find_element_by_xpath('// *[ @ id = "trdDd"]')  # 데이트 픽커
            search_btn = driver.find_element_by_xpath('//*[@id="jsSearchButton"]') #조회 버튼
            for date in dateList:
                datepicker.send_keys(date)
                sleep(1)
                search_btn.click()
                sleep(5)
                dataDownload_btn.click()
                sleep(3)

        dataDownload_btn.click()

        #sleep(2)

        csv_btn = driver.find_element_by_xpath('/html/body/div[2]/section[2]/section/section/div/div/form/div[2]/div[2]/div[2]/div/div[2]/a') # CSV 버튼
        csv_btn.click()

        sleep(5)
        driver.close()

    # 종목정보 업데이트
    def __update_stock_corp_data(self, filepath, date):

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]
        YM_ASSET_STOCK_CTL = stockDB.FS_DB["YM_ASSET_STOCK_CLT"]   #보유종목 컬렉션

        # 파일에서 오늘 종목 데이터(시세 등) 읽기
        list_today_corp_info = self.__read_KRX_stock_daily_info_file(filepath, date, 'EUC-KR')

        # 보유종목
        list_asset_stock = YM_ASSET_STOCK_CTL.find({}, {'_id':0, 'stock_code':1})
        set_asset_stock = set([x['stock_code'] for x in list_asset_stock])

        # DB에 저장된 종목 정보 얻기
        list_exist_corp_info = STOCK_CROP_DATA_CLT.find({},{'_id':1, 'stock_code':1, 'stock_name':1})

        # 현재 종목과 오늘 종목을 비교해서 추가된 종목과 삭제된 종목을 얻는다.
        set_today_stock = set([x['stock_code'] for x in list_today_corp_info])
        set_exist_stock = set([x['stock_code'] for x in list_exist_corp_info])

        set_add_stock = set_today_stock - set_exist_stock
        set_del_stock = set_exist_stock - set_today_stock

        if len(set_del_stock): # 삭제 종목이 있을 경우
            qRslt = STOCK_CROP_DATA_CLT.delete_many({'stock_code':{'$in':list(set_del_stock)}}) #$in 을 사용하기 위해서는 list로 형변환 해주어야함
            print(f'{qRslt.deleted_count}개 종목 삭제: {set_del_stock}')

        if len(set_add_stock): # 추가 종목이 있을 경우
            #list_add_code = [x for x in set_add_stock]
            list_add_corp_info = [x for x in list_today_corp_info if x['stock_code'] in set_add_stock]
            for corp in list_add_corp_info:
                corp['fs_data'] = False
            qRslt = STOCK_CROP_DATA_CLT.insert_many(list_add_corp_info)
            print(f'{len(qRslt.inserted_ids)}개 종목 추가: {set_add_stock}')

        set_update_stock = set_today_stock - set_add_stock
        if len(set_update_stock):
            #list_update_code = [x['stock_code'] for x in set_update_stock]
            list_update_corp_info = [x for x in list_today_corp_info if x['stock_code'] in set_update_stock]

            # 주가정보 업데이트
            list_bulk_qry=[]
            list_asset_stock_qury=[] # 보유종목 주가 업데이트 쿼리
            for doc in list_update_corp_info:
                list_bulk_qry.append(
                    UpdateOne({'stock_code': doc['stock_code']}, {'$set':{'cur_price': doc['cur_price'],
                                    'issued_shares_num': doc['issued_shares_num'],
                                    'price_date': date}})
                )

                # 해당 종목이 보유 종목이라면
                if doc['stock_code'] in set_asset_stock:
                    list_asset_stock_qury.append(
                        UpdateOne({'stock_code':doc['stock_code']},
                                  {
                                      '$set':{'cur_price':doc['cur_price'],'price_date': date}
                                  })
                    )

                if ( len(list_bulk_qry) == 1000 ):
                    STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry,ordered=False)
                    list_bulk_qry = []

            # 전체 종목 가격 업데이트
            if (len(list_bulk_qry) > 0):
                STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)

            # 보유 종목 가격 업데이트
            if(len(list_asset_stock_qury) > 0):
                YM_ASSET_STOCK_CTL.bulk_write(list_asset_stock_qury, ordered=False)

        print(f'{len(set_update_stock)}개 종목 주가 업데이트')

    # KRX 일일 종목정보 파일 읽기
    def __read_KRX_stock_daily_info_file(self, filepath, date, encoding):
        f = open(filepath, 'r', encoding=encoding)
        rows = csv.reader(f)
        listInfo=[] # 반환 값

        cur_price_idx = -1           # 종가 컬럼 인덱스
        issued_shares_num_idx = -1   # 발행주식(상장주식)수 컬럼 인덱스

        for i, row in enumerate(rows):
            if i == 0:
                for j, col in enumerate(row):
                    if col == '종가' or col == '현재가':
                        cur_price_idx = j
                    if col == '상장주식수' or col == '상장주식수(주)':
                        issued_shares_num_idx = j
            else:
                market = row[2]  # 시장구분

                if market == 'KONEX':   #코넥스 종목은 구축대상이 아님
                    continue

                stock_code = row[0]  # 종목코드
                stock_name = row[1]  # 종목명

                cur_price = int(row[cur_price_idx].replace(',', ''))  # 종가
                issued_shares_num = int(row[issued_shares_num_idx].replace(',', ''))  # 발행주식수

                dic_data={'market': market, #시장구분
                            'stock_code':stock_code,    #종목코드
                            'stock_name':stock_name,    #종목명
                            'cur_price': cur_price,     #현재가
                            'issued_shares_num': issued_shares_num, #발행주식수
                            'price_date': date}
                listInfo.append(dic_data)

        return listInfo

    # 폴더 생성 함수
    def __createFolder(self, directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print(f'폴더 생성 실패:{directory}')
            return False
        return True

    # 페이지 다운로드
    def __download_page(self, filename, URL, download_path):
        # URL = f'http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701'
        response = requests.get(URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        with open(f'{download_path}/{filename}.html', 'w', encoding="utf-8") as outfile:
            outfile.write(soup.prettify())
        return
    
    # FnGuid Page 다운로드
    def download_FnGuide_pages(self, mode, download_path="E:/STOCK/download/"):
        print('####### FnGuid 페이지 다운로드 #######')

        # mode: 다운로드 페이지 종류
        # 1: 재무제표 페이지
        # 2: 재무비율 페이지
        # 3: Snapshot 페이지

        # 종목 컬렉션        
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]

        # 종목 정보(리스트)
        stock_info = STOCK_CROP_DATA_CLT.find({})  # 종목

        URL = ""  # 다운로드 대상 페이지 주소

        # 파일 다운로드 폴더 패스
        download_path = 'E:/STOCK/download/'

        now_date = dt.datetime.now().strftime("%Y-%m-%d")

        if mode == 1:  # 제무재표 페이지
            download_path += f'FS/{now_date}'
            print('>>> 재무제표 페이지 다운로드 <<<')
        elif mode == 2:  # 제무비율 페이지
            download_path += f'FR/{now_date}'
            print('>>> 재무비율 페이지 다운로드 <<<')
        elif mode == 3:  # Snapshot 페이지
            download_path += f'CS/{now_date}'
            print('>>> Snapshot 페이지 다운로드 <<<')

        # 다운로드 폴더를 생성해준다.
        self.__createFolder(download_path)

        # 종목 갯수만큼 loop
        down_cnt = 0  # 다운로드 갯수
        for stckInf in stock_info:
            stock_code = stckInf['stock_code']
            stock_name = stckInf['stock_name']
            # 리츠 종목이나 스팩은 제외
            if self.checkCorporation(stock_name) == False:
                continue

            fnm_prefix = ""  # 파일명 접두어
            if mode == 1:  # 제무재표 페이지
                URL = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701'
                fnm_prefix = 'FS'
            elif mode == 2:  # 제무비율 페이지
                URL = f'http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701'
                fnm_prefix = 'FR'
            elif mode == 3:  # Snapshot 페이지
                URL = f'http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701'
                fnm_prefix = 'CS'

            filename = f'{fnm_prefix}_{stock_name}_{stock_code}'  # 다운로드 파일 저장 파일명
            self.__download_page(filename, URL, download_path)
            down_cnt += 1
            print(f'{down_cnt}) {stock_name}')
            time.sleep(0.2)

        print(f'>>> 총({down_cnt})페이지 다운로드 완료 <<<')

    # 문자숫자 -> 숫자
    def __str_to_num(self, strnum):
        if strnum == '':
            return 0
        strnum = strnum.replace(',', '')
        p = re.compile('[+-]?\d*(\.?\d*)$')  # 양/음수를 포함 정수 실수 판별 정규식
        m = p.match(strnum)
        if m == None: return 'N/A'
        strnum = strnum.replace(',', '').strip()
        if strnum.find('.') != -1:
            return float(strnum)
        return int(strnum)

    # 추정 ROE 구하기
    def __estimateROE(self, val1, val2, val3):
        if val1 < val2 and val2 < val3:
            return val3
        if val1 > val2 and val2 > val3:
            return val3
        return (val1 + val2 * 2 + val3 * 3) / 6

    # 컨센서스 내용 파싱
    def __parse_consensus_contents(self, divId, soup):
        # 컨센서스 계정항목: 값
        dic_year = {}
        dic_quarter = {}
        # 컨센서스 헤더 => 년도 및 분기 컨센서스
        tagTrcnsHeader = soup.select(f'#{divId} > table > tbody > tr')
        for tr in tagTrcnsHeader:
            th = tr.select('th')
            accntNm = th[0].text.replace('(원)', '').strip()
            th_span = th[0].select('a > span')
            if th_span:
                accntNm = th_span[0].text.strip()
            tds = tr.select('td')

            if len(tds) < 8: break  # 컨센서스 데이터에서 최근3년 및 최근 3분기 전 데이터가 없다면 자료를 구축하지 않는다.

            yearVal = self.__str_to_num(tds[3].text.strip())
            quarterVal = self.__str_to_num(tds[7].text.strip())

            if yearVal and yearVal != 'N/A':
                dic_year[accntNm] = yearVal
            else:  # 컨센서스 데이터가 없다면 최근 년도 ROE 데이터만 활용
                if accntNm == 'ROE':  # 항목이 ROE라면 추정 ROE를 구한다.
                    val1 = self.__str_to_num(tds[0].text.strip())
                    val2 = self.__str_to_num(tds[1].text.strip())
                    val3 = self.__str_to_num(tds[2].text.strip())
                    if (val1 != 'N/A' and val2 != 'N/A' and val3 != 'N/A'):
                        yearVal = self.__estimateROE(val1, val2, val3)
                        dic_year[accntNm] = yearVal
                elif accntNm == '자본총계' or accntNm == '지배주주지분':
                    yearVal = self.__str_to_num(tds[2].text.strip())
                    if yearVal != 'STRING':
                        dic_year[accntNm] = yearVal

            if quarterVal and quarterVal != 'N/A':
                dic_quarter[accntNm] = quarterVal
            else:  # 컨센서스 데이터가 없다면 최근 분기 ROE 데이터만 활용
                if accntNm == 'ROE':  # 항목이 ROE라면 추정 ROE를 구한다.
                    val1 = self.__str_to_num(tds[4].text.strip())
                    val2 = self.__str_to_num(tds[5].text.strip())
                    val3 = self.__str_to_num(tds[6].text.strip())
                    if (val1 != 'N/A' and val2 != 'N/A' and val3 != 'N/A'):
                        quarterVal = (val1 + val2 * 2 + val3 * 3) / 6  # 분기 데이터 추정의 경우 평균 값 사용
                        dic_quarter[accntNm] = quarterVal
                elif accntNm == '자본총계' or accntNm == '지배주주지분':
                    quarterVal = self.__str_to_num(tds[6].text.strip())
                    if quarterVal != 'STRING':
                        dic_quarter[accntNm] = quarterVal
        # print(json.dumps(dic_year, indent=4, sort_keys=False, ensure_ascii=False))
        # print(json.dumps(dic_quarter, indent=4, sort_keys=False, ensure_ascii=False))
        return dic_year, dic_quarter

    # 컨센서스 데이터 크롤링
    def __crawling_fnGuide_consensus_basic_data(self, filepath):

        with open(filepath, 'r', encoding='utf-8') as file:
            htmlText = file.read()
        soup = BeautifulSoup(htmlText, 'lxml')

        # 컨센서스 계정항목: 값

        # 연결 재무정보
        dic_year, dic_quarter = self.__parse_consensus_contents('highlight_D_A', soup)

        # 해당 종목이 별도 재무정보가

        if dic_year and dic_quarter:
            if dic_year['지배주주지분'] != 0 and dic_quarter['지배주주지분'] == 0:
                # 별도 재무정보
                dic_year, dic_quarter = self.__parse_consensus_contents('highlight_B_A', soup)

        # 자사주
        # treasury_stock = soup.select('#svdMainGrid5 > table > tbody > tr:nth-child(5) > td:nth-child(3)')
        treasury_stock = soup.select('#svdMainGrid5 > table > tbody > tr:nth-of-type(5) > td:nth-of-type(2)')
        if treasury_stock:
            treasury_stock = treasury_stock[0].text.strip()
            treasury_stock = self.__str_to_num(treasury_stock)
        else:
            treasury_stock = 0

        # 배당 수익률
        # dividend_yield = soup.select('#corp_group2 > dl:nth-child(5) > dd')
        # if len(dividend_yield) > 0:
        #     dividend_yield = dividend_yield[0].text.strip()
        #     if dividend_yield == '-': dividend_yield = 0 # 배당수익률이 없는 경우('-')
        #     elif dividend_yield[-1] == '%': # 문자열에 "%" 가 있다면 '%'를 제외
        #         dividend_yield = float(dividend_yield[0:-1])
        # else:
        #     dividend_yield = 0

        # 베타
        beta = soup.select('#svdMainGrid1 > table > tbody > tr:nth-child(4) > td.cle.r')
        if len(beta) > 0:
            beta = beta[0].text.strip()
            if beta == '-': beta = 0
            beta = self.__str_to_num(beta.strip())
        else:
            beta = 0

        # 유동주식수 / 유동비율
        floatStocks=0
        floatStocksR=0
        floatingStockText = soup.select('#svdMainGrid1 > table > tbody > tr:nth-child(7) > td.cle.r')
        if len(floatingStockText) > 0:
            text = floatingStockText[0].text.strip()
            if text.find('/') == -1:
                floatStocks = 0
                floatStocksR = 0
            else:
                floatStocks, floatStocksR = text.split('/')
                floatStocks = self.__str_to_num(floatStocks.strip())
                floatStocksR = self.__str_to_num(floatStocksR.strip())

        # FnGuid 산업분류
        fics = soup.select('#compBody > div.section.ul_corpinfo > div.corp_group1 > p > span.stxt.stxt2')
        if len(fics) > 0:
            text = fics[0].text.strip()
            text = text.replace('FICS', '')
            text = text.replace('&nbsp;','')
            fics = text.strip()
        else: fics="ERR"

        return dic_year, dic_quarter, treasury_stock, beta, floatStocks, floatStocksR, fics

    # 컨센서스 데이터 DB구축
    def constructDB_consensus_data(self, data_dir_path):
        # data_dir_path: snapshot 페이지 html 파일 저장 폴더 경로
        print('####### 컨센서스 데이터 DB 구축 #######')
        # 종목 컬렉션
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]
        CONSENSUS_DATA_CLT = stockDB.FS_DB["CONSENSUS_DATA_CLT"]

        stock_info = STOCK_CROP_DATA_CLT.find({}) # 종목

        cnst_date = data_dir_path.split('/')[-1] #구축날짜, 데이터 저장 경로에서 얻기

        list_bulk=[]    # DB 벌크 처리를 위한 데이터 저장 리스트
        no = 1
        for stckInf in stock_info:
            stock_code = stckInf['stock_code'] # 종목코드
            stock_name = stckInf['stock_name'] # 종목명
            # 리츠 종목이나 스팩, 우선주는 제외
            if self.checkCorporation(stock_name) == False:
                continue

            # 데이터(html) 파일 경로
            filepath = f'{data_dir_path}/CS_{stock_name}_{stock_code}.html' # 다운로드 파일 저장 파일명

            if os.path.isfile(filepath) == False: continue  # 파일이 존재하지 않는다면 스킵

            # 컨센서스 데이터 추출
            # treasury_stock: 자기주식, dividend_yield: 배당수익률
            # beta: 베타계수, floatStocks: 유동주식수, floatStocksR:유동비율, fics: FnGuid 산업분류
            dic_year, dic_quarter, treasury_stock, beta, floatStocks, floatStocksR, fics = self.__crawling_fnGuide_consensus_basic_data(filepath)
            # 분기 컨센서스 정보가 없다면 종목정보가 없음을 의미
            if dic_quarter:
                list_bulk.append([stckInf['stock_code'], dic_year, dic_quarter, treasury_stock, beta, floatStocks, floatStocksR, fics, stock_name])
                print(f'{no}){stock_name} 데이터')
                no += 1

        list_bulk_qry=[] # DB 벌크 처리를 위한  쿼리저장 리스트
        for item in list_bulk:
            list_bulk_qry.append(
                UpdateOne({'stock_code': item[0]}, {'$set':{'cns_year': item[1],
                                'cns_quarter': item[2], 'treasury_stock': item[3],
                                'beta':item[4], 'floatStocks':item[5],
                                'floatStocksR':item[6], 'FICS':item[7]}})
            )
            if ( len(list_bulk_qry) == 1000 ):
                STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry,ordered=False)
                print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축')
                list_bulk_qry = []

        if (len(list_bulk_qry) > 0):
            STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
            print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축')

        list_bulk_qry = []  # DB 벌크 처리를 위한  쿼리저장 리스트
        for item in list_bulk:
            if len(item[1].keys()) < 4: continue    # 컨센서스 데이터가 없다면 구측하지 않는다.
            list_bulk_qry.append(
                InsertOne({'stock_code': item[0],
                           'stock_name': item[8],
                           'date': cnst_date,
                           'cns_year': item[1],
                           'cns_quarter': item[2]})
            )
            if (len(list_bulk_qry) == 200):
                CONSENSUS_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
                print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축')
                list_bulk_qry = []

        if (len(list_bulk_qry) > 0):
            CONSENSUS_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
            print(f'{len(list_bulk_qry)})개 컨센서스 데이터 구축')

        # 컬렉션 인덱스
        CONSENSUS_DATA_CLT.create_index([("date", pymongo.DESCENDING), ("stock_code", pymongo.ASCENDING)], unique=True)

        '''
        ========= tagId =========  
        divSonikY: 포괄손익계산서(년도)
        divSonikQ: 포괄손이계산서(분기)
        divDaechaY: 재무상태표(년도)
        divDaechaQ: 재무상태표(분기) 
        divCashY: 현금흐름표(년도)
        divCashQ: 현금흐름표(분기)
        =========================   
        '''

    # 제무제표 각 파트별 데이터 얻기
    def __acquire_fs_part_data(self, soup, tagId, listPeriod, listFsPartData):
        # head tr 정보 얻기 => 기간 정보 획득
        fsPartTblHeadTr = soup.select(f'#{tagId} > table > thead > tr > th')

        # 손익계산서: 컬럼 총갯수에서 전년동기 2개와 첫 컬럼을 뺀 컬럼갯수가 데이터 수집 대상의 컬럼 갯수임
        # 재무상태표, 현금흐름표: 컬럼 총갯수에서 첫 컬럼을 뺀 컬럼갯수가 데이터 수집 대상의 컬럼 갯수임

        if tagId[-1] == 'Q':  # 분기일 경우
            listTagId = ['divSonikQ', 'divDaechaQ', 'divCashQ']
            idx = listTagId.index(tagId)
            if idx == 0:
                colCnt = len(fsPartTblHeadTr) - 3
            else:
                colCnt = len(fsPartTblHeadTr) - 1

        else:
            listTagId = ['divSonikY', 'divDaechaY', 'divCashY']
            idx = listTagId.index(tagId)
            if idx == 0:
                colCnt = len(fsPartTblHeadTr) - 3
            else:
                colCnt = len(fsPartTblHeadTr) - 1

        for i, th in enumerate(fsPartTblHeadTr):
            if i == 0: continue  # 첫번째 열 제외
            if i > colCnt: break  # 데이터 획득 컬럼을 넘어서면 LOOP 나오기

            if tagId[-1] == 'Q':  # 분기일 경우
                listPeriod.append(th.text.strip())  # '년도/분기' 문자열 모두 저장
            else:
                listPeriod.append(int(th.text.strip().split('/')[0]))  # '년도' 문자열만 저장

            # print(th.text.strip().split('/')[0])

        for i in listPeriod:
            listFsPartData.append({})

        # 계정항목 정보가 포함된 TR 태그 얻기
        accountTrs = soup.select(f'#{tagId} > table > tbody > tr')
        depStartAccntNm = ''  # 상세항목 포함 계정항목 이름
        # accntNo = 0
        # detailAccntNo = 0
        for i, tr in enumerate(accountTrs):
            # print(i+1,')항목:', tr.find('th').text.strip())
            td = tr.select('td')
            listVals = [x.text.strip() for x in td][0:colCnt]  # 기간별 계정항목 비용 값들

            # (상세항목이 없는)일반항목
            if 'rwf' in tr['class'] and \
                    'acd_dep_start_close' not in tr['class'] and \
                    'acd_dep2_sub' not in tr['class']:
                # print(tr.select('th>div')[0].text.strip(), listVals)
                accntNm = tr.select('th > div')[0].text.strip()
                # accntNo += 1
                # accntNm = accntNmPfx+f'_{accntNo}'
                for j, dic in enumerate(listFsPartData):
                    dic[accntNm] = self.__str_to_num(listVals[j])
            # (상세항목을 포함하는)항목
            elif 'acd_dep_start_close' in tr['class']:
                # print("+",tr.select('th>div>span')[0].text.strip(), listVals)
                accntNm = tr.select('th > div > span')[0].text.strip()
                # accntNo += 1
                # accntNm = accntNmPfx+f'_{accntNo}'
                # detailAccntNo = 0
                depStartAccntNm = accntNm + "_상세"
                for j, dic in enumerate(listFsPartData):
                    dic[accntNm] = self.__str_to_num(listVals[j])
                    dic[depStartAccntNm] = {}
            # 상세항목
            elif 'acd_dep2_sub' in tr['class']:
                # print("-", tr.select('th')[0].text.strip(), listVals)
                accntNm = tr.select('th')[0].text.strip()
                # detailAccntNo += 1
                # accntNm = depStartAccntNm+f'_{detailAccntNo}'
                for j, dic in enumerate(listFsPartData):
                    dic[depStartAccntNm][accntNm] = self.__str_to_num(listVals[j])

    # 재무제표 데이터 크롤링
    def __crawling_fnGuide_fs_data(self, filepath, c_year=0, c_quarter=0, mode='a'):
        #URL = "http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A%s&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701" % stock_code
        #response = requests.get(URL)
        #soup = BeautifulSoup(response.text, 'html.parser')
        with open(filepath, 'r', encoding='utf-8') as file:
            htmlText = file.read()
        soup = BeautifulSoup(htmlText, 'lxml')

        '''
        ========= tagId =========  
        divSonikY: 포괄손익계산서(년도)
        divSonikQ: 포괄손이계산서(분기)
        divDaechaY: 재무상태표(년도)
        divDaechaQ: 재무상태표(분기) 
        divCashY: 현금흐름표(년도)
        divCashQ: 현금흐름표(분기)
        =========================   
        '''
        listYearTagId = ['divSonikY', 'divDaechaY', 'divCashY']
        listQuarterTagId = ['divSonikQ', 'divDaechaQ', 'divCashQ']
        listFsPartId = ['포괄손익계산서', '재무상태표', '현금흐름표']

        listYearFsData = []  # 년도별 재무제표 데이터
        listQuarterFsData = []  # 분기별 재무제표 데이터

        # 년도별 재무제표 데이터
        if mode == 'a' or mode == 'y':  # 년도 재무제표 데이터 획득 모드일경우
            for i, tagId in enumerate(listYearTagId):
                # 재무제표(년도)
                listYear = []  # 년도정보
                listYearData = []  # 년도별  재무제표 파트 데이터 ex)[2017년 손익계산서, 2018년 손익계산서, ...]

                #start = time.time()  # 시작 시간 저장
                self.__acquire_fs_part_data(soup, tagId, listYear, listYearData)  # 제무제표 각 파트 별 데이터 획득
                #print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간

                if i == 0:
                    for year in listYear:
                        if c_year != 0 and year != c_year: continue  # 특정 년도 제무재표 데이터만 획득일 경우 해당없는 년도 데이터는 스킵
                        dic_year = {}
                        dic_year['year'] = year
                        listYearFsData.append(dic_year)

                for j, year in enumerate(listYear):
                    if c_year == 0:  # 모든 재무제표 데이터 획득 일경우
                        listYearFsData[j][listFsPartId[i]] = listYearData[j]
                    else:  # 특정년도 재무제표 데이터만 획득일 경우
                        if year == c_year:  # 해당 재무제표 데이터 만 저장
                            listYearFsData[0][listFsPartId[i]] = listYearData[j]

        # print(json.dumps(listYearFsData[0], indent=4, sort_keys=False, ensure_ascii=False))

        # 분기별 재무제표 데이터
        if mode == 'a' or mode == 'q':  # 분기 재무제표 데이터 획득 모드일경우
            for i, tagId in enumerate(listQuarterTagId):
                # 재무제표(분기)
                listYearQuarter = []  # 년도/분기정보
                listQuarterData = []  # 분기별 재무제표 파트 데이터 ex)[2017년 손익계산서, 2018년 손익계산서, ...]
                self.__acquire_fs_part_data(soup, tagId, listYearQuarter, listQuarterData)

                if i == 0:
                    for yearQuarter in listYearQuarter:
                        dic_year = {}
                        [year, quarter] = yearQuarter.split('/')
                        year = self.__str_to_num(year)
                        quarter = int(self.__str_to_num(quarter) / 3)
                        if c_quarter != 0 and \
                                (
                                        year != c_year or quarter != c_quarter): continue  # 특정 년도/분기 제무재표 데이터만 획득일 경우 해당없는 년도 데이터는 스킵
                        dic_year['year'] = year
                        dic_year['quarter'] = quarter
                        listQuarterFsData.append(dic_year)

                for j, yearQuarter in enumerate(listYearQuarter):
                    [year, quarter] = yearQuarter.split('/')
                    year = self.__str_to_num(year)
                    quarter = int(self.__str_to_num(quarter) / 3)
                    if c_quarter == 0:  # 모든 재무제표 데이터 획득 일경우
                        listQuarterFsData[j][listFsPartId[i]] = listQuarterData[j]
                    else:  # 특정년도 재무제표 데이터만 획득일 경우
                        if year == c_year and quarter == c_quarter:  # 해당 재무제표 데이터 만 저장
                            listQuarterFsData[0][listFsPartId[i]] = listQuarterData[j]

        return listYearFsData, listQuarterFsData
        # print(json.dumps(listQuarterFsData[0], indent=4, sort_keys=False, ensure_ascii=False))

        # with open('fnGuid_sample.html', 'w', encoding="utf-8") as outfile:
        #    outfile.write(soup.prettify())


    # 기업종목이 아닌 종목들 확인
    def checkCorporation(self, stock_name):
        if stock_name.endswith('리츠') or stock_name.find('스팩') > 0 or stock_name[-1] == "우" or\
                stock_name.find('유안타제') >= 0 or stock_name.find('신한제') >= 0 or stock_name.find('대신밸런스제') >= 0 or\
                stock_name.find('IBKS제') >= 0 or stock_name.find('케이비제') >= 0 or stock_name.endswith('우B') or\
                stock_name.endswith('우(전환)') or stock_name.find('하나머스트') >= 0:
            return False
        return True


    # 재무제표 데이터 DB구축
    def constructDB_financialStatement_data(self, data_dir_path, year=0, quarter=0, mode='a', target_stock_code='all'):
        '''
        year: 0 fn가이드에서 제공하는 모든 년도 재무제표 데이터 구축
             ex)2020: 2020년도 재무제표 데이터 구축
        quarter: 0 fn가이드에서 제공하는 모든 분기 재무제표 데이터 구축
                ex)3: 3분기 재무제표 데이터 구축
        mode: 'y': 년도 재무제표 데이터만 구축
              'q': 분기 재무제표 데이터만 구축
              'a': 년도/분기 재무제표 데이터 모두 구축
        '''

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]
        YEAR_FS_DATA_CLT = stockDB.FS_DB["YEAR_FS_DATA_CLT"]
        QUARTER_FS_DATA_CLT = stockDB.FS_DB["QUARTER_FS_DATA_CLT"]
        stock_info = STOCK_CROP_DATA_CLT.find({})  # 종목

        list_bulk_year = []
        list_bulk_quarter = []

        no = 1
        for i, stckInf in enumerate(stock_info):
            stock_code = stckInf['stock_code']

            # 특정 종목 획득일 경우 해당 종목이 아닌 데이터는 스킵한다.
            if target_stock_code != 'all' and \
                    target_stock_code != stock_code: continue

            stock_name = stckInf['stock_name']

            if self.checkCorporation(stock_name) == False:
                continue

            # 데이터(html) 파일 경로
            filepath = f'{data_dir_path}/FS_{stock_name}_{stock_code}.html'  # 다운로드 파일 저장 파일명

            if os.path.isfile(filepath) == False: continue

            #start = time.time()  # 시작 시간 저장
            listYearFsData, listQuarterFsData = self.__crawling_fnGuide_fs_data(filepath, year, quarter, mode)
            #print("실행시간(초) :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간

            if len(listYearFsData):
                for x in listYearFsData:
                    x['stock_code'] = stock_code
                    x['stock_name'] = stock_name
                    list_bulk_year.append(x)

            if len(listQuarterFsData):
                for x in listQuarterFsData:
                    x['stock_code'] = stock_code
                    x['stock_name'] = stock_name
                    list_bulk_quarter.append(x)

            now = dt.datetime.now()
            stock_nm = stckInf['stock_name']
            print(f'{no}){stock_nm} 년도:{len(listYearFsData)}개,분기:{len(listQuarterFsData)}개 crawling({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
            no += 1
            #time.sleep(0.5)

        list_bulk_pair = [list_bulk_year, list_bulk_quarter]
        list_clt = [YEAR_FS_DATA_CLT, QUARTER_FS_DATA_CLT]
        list_data_name = ['년도 재무제표', '분기 재무제표']

        for i, list_bulk in enumerate(list_bulk_pair):
            list_bulk_qry = []

            for item in list_bulk:
                list_bulk_qry.append(InsertOne(item))
                if (len(list_bulk_qry) == 1000):
                    list_clt[i].bulk_write(list_bulk_qry, ordered=False)
                    print(
                        f'{len(list_bulk_qry)})개 {list_data_name[i]} 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')
                    list_bulk_qry = []

            if (len(list_bulk_qry) > 0):
                list_clt[i].bulk_write(list_bulk_qry, ordered=False)
                print(
                    f'{len(list_bulk_qry)})개 {list_data_name[i]} 데이터 구축 ({now.year}-{now.month}-{now.day} {now.hour}:{now.minute}:{now.second})')


    # 제무비율 데이터 크롤링
    def __crawling_fnGuide_FR_data(self, filepath, c_year=0, c_quarter=0):
        with open(filepath, 'r', encoding='utf-8') as file:
            htmlText = file.read()
        soup = BeautifulSoup(htmlText, 'html.parser')

        # 제무비율 기간 컬럼헤드 ex) 2017/12, 2018/12, ...

        trs = soup.select('div.um_table > table > thead > tr')
        if len(trs) == 0:
            return pd.DataFrame(), pd.DataFrame()

        colHeadYear = soup.select('div.um_table > table > thead > tr')[0].select('th')
        colHeadQuat = soup.select('div.um_table > table > thead > tr')[1].select('th')
        # 제무비율 데이터 년도/분기 정보 => DataFrame 인덱스로 사용
        listYearIndex = [x.text.strip() for i, x in enumerate(colHeadYear) if i != 0]
        listQuatIndex = [x.text.strip() for i, x in enumerate(colHeadQuat) if i != 0]

        dicFR = {}  # 데이터 저장용 딕셔너리

        divs = soup.select('div.um_table')
        for i, div in enumerate(divs):
            if i == 1:  # 년도 데이터 수집 완료, 분기데이터 수집 시작
                yKeys = dicFR.keys()
                listYearFrInfo = [pd.DataFrame(dicFR[x]) for x in yKeys]  # 제무비율 데이터 dictionary 형식 -> dataframe 변환
                for df in listYearFrInfo: df.index = listYearIndex  # 데이터 프레임에 index 값 설정, [기간정보, ex) 2016/12, 2017/12, ..]
                # 분기 제무비율 데이터 수집을 위해 데이터 저장용 딕셔너리 초기화
                dicFR = {}
            trs = div.select('table > tbody > tr')
            for tr in trs:
                if tr['class'][0] == 'tbody_tit':  # 제무비율 구분 타이틀(안정성, 성장성, 수익성, 활동성)
                    ratioTitle = tr.select('th')[0].text.strip()
                    # print(ratioTitle)

                    # 제무비율 구분 타이틀로 키값 추가
                    if ratioTitle not in dicFR:
                        dicFR[ratioTitle] = {}

                else:
                    sub_rAccntNm = ''
                    if len(tr['class']) > 1:
                        if tr['class'][1] == 'acd_dep_start_close':  # 항목명 / 항목 구성 요소 명
                            span = tr.select('th > div > div > a > span')
                            if len(span) > 0:
                                rAccntNm = tr.select('th > div > div > a > span')[0].text.strip()
                            else:
                                rAccntNm = tr.select('th > div > div')[0].text.strip()
                            rAccntNm = rAccntNm.replace('&nbsp;', '')
                            # print('  ' + rAccntNm)
                        elif tr['class'][2] == 'acd_dep2_sub':  # 서브 항목 명명
                            t1 = tr.select('th > div > dl > dt')
                            if len(t1) == 0:
                                t1 = tr.select('th > div')
                            sub_rAccntNm = t1[0].text.strip()
                            sub_rAccntNm = sub_rAccntNm.replace('\n', '')
                            sub_rAccntNm = sub_rAccntNm.replace(' ', '')
                            # print('    ' + sub_rAccntNm)
                    else:
                        rAccntNm = tr.select('th > div')[0].text.strip()
                        # print('  ' + rAccntNm)

                    # 항목의 년도 별 데이터 dictionary
                    dicAccnt = dicFR[ratioTitle]
                    # 항목 데이터 태그
                    tds = tr.select('td')
                    # 태그에서 텍스트 데이터 추출
                    vals = [x.text.strip() for x in tds]

                    # 값을 숫자형으로 변환('N/A', '흑전', '적지' 는 제외)
                    for j, val in enumerate(vals):
                        if val != 'N/A' and val != '흑전' and val != '적지':
                            vals[j] = self.__str_to_num(val)

                    # 텍스트 데이터 년도별 dic 에 저장
                    key = rAccntNm
                    if sub_rAccntNm != '':  # 서브 타이틀이 존재한다면
                        key = rAccntNm + '_' + sub_rAccntNm  # 타이틀_서브타이틀 형식

                    dicAccnt[key] = vals

        # 분기 제무비율 데이터
        yKeys = dicFR.keys()
        listQuatFrInfo = [pd.DataFrame(dicFR[x]) for x in yKeys]  # 제무비율 데이터 dictionary 형식 -> dataframe 변환
        for df in listQuatFrInfo: df.index = listQuatIndex  # 데이터 프레임에 index 값 설정, [기간정보, ex) 2016/3, 2016/6, ..]

        if c_year != 0:  # 데이터 모두 구축이 아니라면 => 특정 년도 데이터 만 구축
            # 지정 년도에 해당하는 재무비율 데이터 추출
            # 재무비율 데이터 프레임의 인덱스 값에서 지정 년도에 해당하는 인덱스(행)을 추출
            for y in listYearIndex:
                if y.find(str(c_year)) == 0:
                    listYearFrInfo = [x.loc[[y]] for x in listYearFrInfo]
                    break

        if c_quarter != 0:  # 데이터 모두 구축이 아니라면 => 특정 분기 데이터 만 구축
            # 지정 분기에 해당하는 재무비율 데이터 추출
            # 재무비율 데이터 프레임의 인덱스 값에서 지정 년도에 해당하는 인덱스(행)을 추출
            for q in listQuatIndex:
                t = (str(c_year) + '/%02d' % (c_quarter*3))
                if q == t:
                    listQuatFrInfo = [x.loc[[y]] for x in listQuatFrInfo]
                    break

        # 년도 제무비율 데이터 데이터 프레임의 열->인덱스, 인덱스->열 로 변경
        for i, df in enumerate(listYearFrInfo):
            df = df.stack()  # 열 -> 인덱스
            listYearFrInfo[i] = df.unstack(0)  # 인덱스 ->열

        # 년도 제무비율 데이터(안정성비율, 성장성비율, 수익성비율, 활동성비율) 데이터를 하나의 데이터 프레임으로 합친다.(concat 함수 사용)
        dfYearFr = pd.DataFrame()
        if len(listYearFrInfo) > 0:
            dfYearFr = listYearFrInfo[0]
            for i, df in enumerate(listYearFrInfo):
                if i < len(listYearFrInfo) - 1:
                    dfYearFr = pd.concat([dfYearFr, listYearFrInfo[i + 1]])

        # 분기도 제무비열 데이터 데이터 프레임의 열->인덱스, 인덱스->열 로 변경
        for i, df in enumerate(listQuatFrInfo):
            df = df.stack()  # 열 -> 인덱스
            listQuatFrInfo[i] = df.unstack(0)  # 인덱스 ->열

        # 분기 제무비율 데이터(성장성비율, 수익성비율) 데이터를 하나의 데이터 프레임으로 합친다.(concat 함수 사용)
        dfQuatFr = pd.DataFrame()
        if len(listQuatFrInfo) > 0:
            dfQuatFr = listQuatFrInfo[0]
            for i, df in enumerate(listQuatFrInfo):
                if i < len(listQuatFrInfo) - 1:
                    dfQuatFr = pd.concat([dfQuatFr, listQuatFrInfo[i + 1]])

        return dfYearFr, dfQuatFr

        # print(json.dumps(listQuarterFsData[0], indent=4, sort_keys=False, ensure_ascii=False))

        # with open('fnGuid_sample.html', 'w', encoding="utf-8") as outfile:
        #    outfile.write(soup.prettify())


    # 재무비율 데이터 DB구축
    def constructDB_financialRatio_data(self, data_dir_path, year=0, quarter=0, insertYear=True, insertQuar=True, mode='a', target_stock_code='all'):
        '''
        year: 0 fn가이드에서 제공하는 모든 년도 재무비율 데이터 구축
             ex)2020: 2020년도 재무비율 데이터 구축
        quarter: 0 fn가이드에서 제공하는 모든 분기 재무비율 데이터 구축                ex)3: 3분기 재무비율 데이터 구축
        mode: 'y': 년도 재무비율 데이터만 구축
              'q': 분기 재무비율 데이터만 구축
              'a': 년도/분기 재무비율 데이터 모두 구축
        '''

        # 벌크 쿼리 생성
        def bulk_query(listQuery, CLT, dfFr, mode, insert, filterYQ=0):
            # filterYQ: 데이터를 연도, 분기의 특정 데이터로 필터링, 0: 필터링 없음
            if dfFr.empty == True: return

            listCols = dfFr.columns

            for col in listCols:
                df = dfFr.loc[:, [col]]
                dic = df.to_dict('dict')
                key = list(dic.keys())[0]
                dicFrVal = dic[key]
                dicFr = {}
                dicFr['stock_code'] = stock_code
                dicFr['stock_name'] = stock_name
                if key.find('/') != -1:
                    year, mon = key.split('/')
                elif key.find('.') != -1:
                    year, mon = key.split('.')

                if mode == 'y' and filterYQ != 0:  # 특정 연도 데이터만 추출 시
                    if int(year) != filterYQ:  # 현 데이터가 지정 연도데이터와 일치하지 않을 시 스킵
                        continue

                if mode == 'q' and filterYQ != 0:  # 특정 분기 데이터만 추출 시
                    if int(mon) != filterYQ * 3:  # 현 데이터가 지정 분기데이터와 일치하지 않을 시 스킵, (*추출 데이터가 월 임으로 분기에 3을 곱하여 비교)
                        continue

                dicFr['year'] = year
                dicFr['FR'] = dicFrVal
                if mode == 'q':
                    dicFr['month'] = mon
                if insert == True:
                    listQuery.append(InsertOne(dicFr))
                else:
                    if mode == 'y':
                        listQuery.append(
                            UpdateOne({'stock_code': stock_code, 'year': year}, {'$set': {'FR': dicFrVal}})
                        )
                        # print(f'{len(listQuery)}) {year}년 {stock_name}추가')

                    else:
                        listQuery.append(
                            UpdateOne({'stock_code': stock_code, 'year': year, 'month': mon},
                                      {'$set': {'FR': dicFrVal}})
                        )

                if len(listQuery) == 1000:
                    CLT.bulk_write(listQuery, ordered=False)
                    print(f'{len(listQuery)})개 데이터 구축')
                    listQuery = []

            return listQuery

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]
        YEAR_FR_DATA_CLT = stockDB.FS_DB["YEAR_FR_DATA_CLT"]  # 년도 제무비율 데이터
        QUARTER_FR_DATA_CLT = stockDB.FS_DB["QUARTER_FR_DATA_CLT"]  # 분기 간이 제무비율 데이터

        stock_info = STOCK_CROP_DATA_CLT.find({})  # 종목

        listYearQuery = []
        listQuatQuery = []

        no = 1
        for i, stckInf in enumerate(stock_info):
            stock_code = stckInf['stock_code']
            stock_name = stckInf['stock_name']
            if self.checkCorporation(stock_name) == False:
                continue

            # if i < 1402: continue

            # if i == 1599: continue

            # 특정 종목 획득일 경우 해당 종목이 아닌 데이터는 스킵한다.
            if target_stock_code != 'all' and \
                    target_stock_code != stock_code: continue

            if self.checkCorporation(stock_name) == False:
                continue

            # 년도, 분기 제무비율 정보 얻기
            # dfYearFr, dfQuatFr = crawling_fnGuide_FR_data(stock_code, stock_name, True, year, quarter)

            # 데이터(html) 파일 경로
            filepath = f'{data_dir_path}/FR_{stock_name}_{stock_code}.html' # 다운로드 파일 저장 파일명

            if os.path.isfile(filepath) == False: continue  # 파일이 존재하지 않는다면 스킵

            dfYearFr, dfQuatFr = self.__crawling_fnGuide_FR_data(filepath, year, quarter)

            if dfYearFr.empty == True and dfQuatFr.empty == True:
                continue

            if mode == 'y' or mode == 'a':
                listYearQuery = bulk_query(listYearQuery, YEAR_FR_DATA_CLT, dfYearFr, 'y', insertYear, year)

            if mode == 'q' or mode == 'a':
                listQuatQuery = bulk_query(listQuatQuery, QUARTER_FR_DATA_CLT, dfQuatFr, 'q', insertQuar, quarter)

            if target_stock_code != 'all': break

            print(f'{no}){stock_name} 재무비율')
            no += 1

        if (len(listYearQuery) > 0):
            YEAR_FR_DATA_CLT.bulk_write(listYearQuery, ordered=False)
            print(f'{len(listYearQuery)})개 데이터 구축')

        if (len(listQuatQuery) > 0):
            QUARTER_FR_DATA_CLT.bulk_write(listQuatQuery, ordered=False)
            print(f'{len(listQuatQuery)})개 데이터 구축')

    # 거래량 증가 비율 TOP 200 종목 중 SRIM 100 이하 종목 기록
    def writeUnderSrim100InVolumeRTop200(self):
        
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]

        ld_ratio100 = lambda x, y: x / y * 100
        cursCorp = STOCK_CROP_DATA_CLT.find({})
        
        df = pd.DataFrame()

        for docStock in cursCorp:  # 종목정보
            idx = docStock['stock_code']
            cur_price = docStock['cur_price']

            dictRow = {}  # 데이터프레임의 ROW => 개별 종목 정보

            dictRow['date'] = (docStock['date'])
            dictRow['stock_code'] = idx
            dictRow['stock_name'] = docStock['stock_name']
            dictRow['market'] = docStock['market']
            dictRow['cur_price'] = (cur_price)
            dictRow['srim100'] = 0
            dictRow['srim100_PR'] = 0

            if 'S-RIM' in docStock:
                dictRow['srim100'] = (docStock['S-RIM']['100'])
                if dictRow['srim100'] != 0:
                    dictRow['srim100_PR'] = ld_ratio100(cur_price, dictRow['srim100'])

            if "floatStocks" in docStock: #유동주식 정보가 있다면
                dictRow['floatStocks'] = (docStock["floatStocks"]) # 유동주식수
            else:   # 유동주식 정보가 없다면 전체주식 대입
                dictRow['floatStocks'] = np.int64(docStock["issued_shares_num"])  # 전체주식수

            dictRow['volumn_TR'] = 0 #유동주식 대비 거래량 비율

            dictRow['market_cap'] = round(cur_price*docStock["issued_shares_num"] / 100000000)  # 시가총액 억단위로 환산
            dictRow['last_price'] = (docStock["전일종가"])  # 전일가
            dictRow['cur_volumn'] = np.int64(docStock["거래량"])  # 거래량
            dictRow['last_volumn'] = np.int64(docStock["전일거래량"])  # 전일 거래량
    
            if dictRow['floatStocks'] != 0:
                dictRow['volumn_TR'] = ld_ratio100(dictRow['cur_volumn'], dictRow['floatStocks'])  # 유동주식 대비 거래량 비율
                
            df = df.append(dictRow, ignore_index=True) # 데이터 프레임에 ROW추가

        #df.set_index('stock_code', inplace=True)
        df.sort_values(by=['volumn_TR'], axis=0, ascending=False, inplace=True)

        # df = df.astype({'cur_price':'int',
        #            'cur_volumn':'int',
        #            'date':'int',
        #            'floatStocks':'int',
        #            'last_price':'int',
        #            'last_volumn':'int',
        #            'srim100':'int',
        #            'market_cap':'int'})

        bulk_query=[]

        for i, idx in enumerate(df.index):
            if i >= 200: break # 거래비율 상위 200 종목내에서 S-RIM100 보다 저평가된 종목을 찾는다.
            if df.loc[idx]['srim100_PR'] < 100 and df.loc[idx]['srim100_PR'] != 0:
                # 데이터 입력
                item = df.loc[idx].to_dict()
                item['cur_price'] = int(item['cur_price'])
                item['cur_volumn'] = int(item['cur_volumn'])
                item['date'] = int(item['date'])
                item['floatStocks'] = int(item['floatStocks'])
                item['last_price'] = int(item['last_price'])
                item['last_volumn'] = int(item['last_volumn'])
                item['srim100'] = int(item['srim100'])
                item['market_cap'] = int(item['market_cap'])

                bulk_query.append(InsertOne(item))

        stockDB.FS_DB["TOP_VOLUMN_RATIO_SRIM100"].bulk_write(bulk_query, ordered=False)
        stockDB.FS_DB["TOP_VOLUMN_RATIO_SRIM100"].create_index([("date", pymongo.DESCENDING), ("stock_code", pymongo.ASCENDING)], unique=True)
        print("TOP_VOLUMN_RATIO_SRIM100 완료!")
        
    # 거래량 평균대비 폭증 종목 확인
    def writeVolumnJumpStocks(self,meanDays, volMultiStd):
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]

        ld_ratio100 = lambda x, y: x / y * 100
        cursCorp = STOCK_CROP_DATA_CLT.find({})

        dfResult = pd.DataFrame()

        for docStock in cursCorp:  # 종목정보
            stock_code = docStock["stock_code"]
            today = docStock["date"]
            #{"date": {"$lt": today}}
            cursPrice = stockDB.SP_DB["A"+stock_code].find({}).sort("날짜",pymongo.DESCENDING).limit(meanDays+1)
            ltPriceInf = list(cursPrice)
            cur_volumn = ltPriceInf[0]["거래량"]
            del ltPriceInf[0]

            df = pd.DataFrame(ltPriceInf)
            volumnMean = df["거래량"].mean() # 20일 평균 거래량

            if volumnMean < 1000:continue

            dictRow = {}  # 데이터프레임의 ROW => 개별 종목 정보

            if (cur_volumn / volumnMean) > volMultiStd:
                stock_code = docStock['stock_code']
                cur_price = docStock['cur_price']
                dictRow['date'] = (docStock['date'])
                dictRow['stock_code'] = stock_code
                dictRow['stock_name'] = docStock['stock_name']
                dictRow['market'] = docStock['market']
                dictRow['cur_price'] = cur_price
                dictRow['last_price'] = (docStock["전일종가"])  # 전일가
                dictRow['srim100'] = 0
                dictRow['srim100_PR'] = 0

                if 'S-RIM' in docStock:
                    dictRow['srim100'] = (docStock['S-RIM']['100'])
                    if dictRow['srim100'] != 0:
                        dictRow['srim100_PR'] = ld_ratio100(cur_price, dictRow['srim100'])

                dictRow['market_cap'] = round(cur_price * docStock["issued_shares_num"] / 100000000)  # 시가총액 억단위로 환산

                dictRow['cur_volumn'] = np.int64(docStock["거래량"])  # 거래량
                dictRow['last_volumn'] = np.int64(docStock["전일거래량"])  # 전일 거래량

                dictRow['meanDays'] = meanDays
                dictRow['volumnMean'] = volumnMean
                dictRow['volMultiple'] = (cur_volumn / volumnMean)

                dfResult = dfResult.append(dictRow, ignore_index=True)  # 데이터 프레임에 ROW추가

        bulk_query=[]
        for i, idx in enumerate(dfResult.index):
            # 데이터 입력
            item = dfResult.loc[idx].to_dict()
            item['date'] = int(item['date'])
            item['cur_price'] = int(item['cur_price'])
            item['cur_volumn'] = int(item['cur_volumn'])
            item['last_price'] = int(item['last_price'])
            item['last_volumn'] = int(item['last_volumn'])
            item['srim100'] = int(item['srim100'])
            item['market_cap'] = int(item['market_cap'])
            item['meanDays'] = int(item['meanDays'])

            bulk_query.append(InsertOne(item))

        stockDB.FS_DB["JUMP_VOLUMN_RATIO"].bulk_write(bulk_query, ordered=False)
        stockDB.FS_DB["JUMP_VOLUMN_RATIO"].create_index(
            [("date", pymongo.DESCENDING), ("stock_code", pymongo.ASCENDING)], unique=True)
        print("JUMP_VOLUMN_RATIO 완료!")

    def __crawlNaverNewPageHtml(self, stock_code, pageNo):
        URL = f"https://finance.naver.com/item/news_news.nhn?code={stock_code}&page={pageNo}&sm=title_entity_id.basic&clusterId="
        response = requests.get(URL)
        soup = BeautifulSoup(response.text, 'lxml')
        return soup

    # 네이버 종목 뉴스 얻기
    def crawlingNaverStockNewsInfo(self):

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]

        cursCorp = STOCK_CROP_DATA_CLT.find({})

        stock_no = 1 # 종목 no
        for docStock in cursCorp:  # 종목정보

            stock_code = docStock["stock_code"]
            stock_name = docStock["stock_name"]

            #if stock_code != '000850': continue

            createNewClt = False  # 컬렉션 신규 생성 여부
            listCltNames = stockDB.NEWS_DB.list_collection_names()
            if "A"+stock_code not in listCltNames:
                createNewClt = True # 컬렉션 신규 생성 여부

            STOCK_NEWS_CLT = stockDB.NEWS_DB["A"+stock_code] #종목 뉴스정보 컬랙션

            if createNewClt == False: # 컬렉션에 데이터가 존재한다면, 마지막 뉴스의 날짜를 구한다.
                lastDateDoc = STOCK_NEWS_CLT.find_one(sort=[("date", pymongo.DESCENDING)]) #뉴스 마지막 날짜
                if lastDateDoc == None:  # 컬렉션에 데이터가 없는 경우
                    lastDate = None
                else:
                    lastDate = lastDateDoc["date"]

            #list_insertNew = []  # 추가될 문서(뉴스정보)
            set_insertNews = set() # 중복뉴스 입력 방지용
            insertedCnt = 0 #추가된 뉴스 개수

            for pageNo in range(1, 11): # 뉴스 게시판의 기본 페이지 수 만큼만 반복, 1일치 뉴스가 2페이지 이상 발생되지 않는다고 가정함
                soup = self.__crawlNaverNewPageHtml(stock_code, pageNo)

                newsTitle = soup.select('body > div > table.type5 > tbody td.title > a')    # 뉴스 타이틀
                newsInfo = soup.select('body > div > table.type5 > tbody td.info')          # 뉴스 출처
                newsDate = soup.select('body > div > table.type5 > tbody td.date')          # 뉴스 날짜

                list_newsTitle = [x.text.strip() for x in newsTitle]
                list_newsInfo = [x.text.strip() for x in newsInfo]
                list_newsDate = [x.text.strip()[:10] for x in newsDate] # 날짜까지만 추출
                list_newsUrl = [ "https://finance.naver.com"+x["href"].strip() for x in newsTitle] #뉴스 URL
                
                if len(list_newsTitle) == 0: break # 크롤링한 데이터가 없을 경우 루프 종료

                bBreakPageLoop = False  # 뉴스 페이지 loop 종료 플래그

                for title, info, date, url in zip(list_newsTitle, list_newsInfo, list_newsDate, list_newsUrl):
                    doc ={"title":title, "info":info, "date":date, "url":url}
                    # 컬렉션 신규 생성이 아니고 컬렉션에 데이터가 존재한다면
                    # => 기존 뉴스의 마지막 날짜 이후 뉴스만 컬렉션에 추가
                    if createNewClt == False and lastDate != None:
                        ilastDate = int(lastDate.replace(".", ""))
                        idate = int(date.replace(".", ""))
                        if idate < ilastDate: # 해당 뉴스가 저장된 마지막 뉴스의 날짜보다 이전, 더이상 새로운 뉴스는 없음 페이지 루프 종료
                            bBreakPageLoop = True # 뉴스 페이지 루프 종료
                            break  # 마지막 저장 뉴스 날짜보다 이전 뉴스는 입력하지 않음

                    # 컬렉션 유니크 키(title+info+date)가 존재함으로
                    # 종복 뉴스 입력방지를 위해 key값 set으로 뉴스의 중복 체크
                    duplKey = title+info+date
                    if duplKey in set_insertNews:
                        if len(list_newsTitle) == 1: # 페이지 별 뉴스 1개 전시되는 오류 대응 => 페이지 루프 종료
                            bBreakPageLoop = True  # 뉴스 페이지 루프 종료
                            break
                        continue
                    else: set_insertNews.add(duplKey)
                    #list_insertNew.append(doc)

                    # 저장된 마지막 뉴스와 같은 날짜 뉴스의 경우 중복 확인을 하고 뉴스를 추가하고
                    # 이후 날짜 뉴스는 바로 추가 수행
                    if idate == ilastDate:
                        edoc = STOCK_NEWS_CLT.find_one({"title":title, "info":info, "date":date})
                        if edoc == None:
                            STOCK_NEWS_CLT.insert_one(doc)
                            insertedCnt += 1
                    elif idate > ilastDate:
                        STOCK_NEWS_CLT.insert_one(doc)
                        insertedCnt += 1

                if bBreakPageLoop:
                    break

                time.sleep(0.2)

            if createNewClt == True: # 신규 컬렉션의 경우 인덱스 설정
                STOCK_NEWS_CLT.create_index([("date", pymongo.DESCENDING)],unique=False)

            print(f'{stock_no}) [{stock_code}]{stock_name}, {insertedCnt}개 뉴스 추가')
            stock_no+=1

    # 뉴스 DB에 중복 입력된 데이터 제거(초기 DB구축 시 오입력 수정용)
    def removeDuplicateDocInNaverNewDB(self):
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]

        cursCorp = STOCK_CROP_DATA_CLT.find({})

        no = 1
        for docStock in cursCorp:  # 종목정보
            stock_code = docStock["stock_code"]
            stock_name = docStock["stock_name"]

            STOCK_NEWS_CLT = stockDB.NEWS_DB["A" + stock_code]  # 종목 뉴스정보 컬랙션
            list_index = STOCK_NEWS_CLT.list_indexes()
            for index in list_index: # 인덱스 확인 후 삭제
                #print(type(index))
                #print(index)
                if index['name'] == "url_1":
                    STOCK_NEWS_CLT.drop_index("url_1")

            cursor = STOCK_NEWS_CLT.aggregate([
                {"$group": {"_id": {'title': '$title', 'info': '$info', 'date': '$date'}, "dups": {"$push": "$_id"},
                            "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}}
            ])

            for doc in cursor:
                list_dups = doc["dups"][1:] # 중복id 리스트의 첫번째 id을 제외한 나머지
                STOCK_NEWS_CLT.delete_many({"_id": {"$in": list_dups}}) # 중복 id 제거

            # cursor = STOCK_NEWS_CLT.find({})
            # for doc in cursor:
            #     url = doc["url"].replace("https://finance.naver.com//", "https://finance.naver.com/")
            #     STOCK_NEWS_CLT.update_one({"_id":doc["_id"]},{'$set': {'url': url}})

            # STOCK_NEWS_CLT.create_index(
            #     [("title", pymongo.ASCENDING), ("info", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], unique=True)
            # STOCK_NEWS_CLT.create_index(
            #     [("date", pymongo.DESCENDING)], unique=False)
            print(f'{no}) {stock_code} {stock_name}')
            no += 1


    # 현경컨센서스 리포트정보 얻기
    def crawlingHKConsensusReportInfo(self, startDate, endDate):

        listCltNames = stockDB.FS_DB.list_collection_names()
        if "REPORT_CLT" not in listCltNames:
            createNewClt = True  # 컬렉션 신규 생성 여부
        else:
            createNewClt = False  # 컬렉션 신규 생성 여부

        STOCK_REPORT_CLT = stockDB.FS_DB["REPORT_CLT"]  # 종목 리포트 정보 컬랙션

        options = webdriver.ChromeOptions()
        #options.add_argument('headless')
        #options.add_argument('disable-gpu')
        options.add_argument('window-size=1920,1080')
        driver = webdriver.Chrome(executable_path='./resources/webdriver/chromedriver', options=options)
        driver.implicitly_wait(2)
        driver.get(url=f"http://consensus.hankyung.com/apps.analysis/analysis.list?&sdate={startDate}&edate={endDate}&report_type=CO&order_type=&now_page=1")

        lastPageBtn = driver.find_element_by_css_selector('#contents > div.paging > a.btn.last')
        if lastPageBtn != None:
            href = lastPageBtn.get_property("href")
            list_match = re.findall(".*now_page=(\d*)", href)
            if len(list_match) > 0:
                totalPage = int(list_match[0])
            else: return
        else: return

        # 종목명과 종목코드 추출을 위한 정규식 패턴
        rPattern = re.compile("(.*)\((.*?)\)")

        for pageNo in range(1, totalPage+1):
            if pageNo > 1:

                script = f'''
                    $("<a id='page_locationkjg01' href='/apps.analysis/analysis.list?&sdate={startDate}&edate={endDate}&report_type=CO&order_type=&now_page={pageNo}'>이동</a>").appendTo('body');
                '''
                driver.execute_script(script)
                aTag = driver.find_element_by_id('page_locationkjg01')
                aTag.click()

                # driver.get(
                #    url=f"http://consensus.hankyung.com/apps.analysis/analysis.list?&sdate={startDate}&edate={endDate}&report_type=CO&order_type=&now_page={pageNo}")
                sleep(1)

            # 작성일
            elements = driver.find_elements_by_css_selector('#contents > div.table_style01 > table > tbody > tr > td.first.txt_number') # 작성일
            list_writeDate = [x.text for x in elements]
            # 리포트 제목
            elements = driver.find_elements_by_css_selector('#contents > div.table_style01 > table > tbody > tr > td.text_l > div > div > strong') # 리포트 제목
            list_title = [x.get_attribute("innerHTML") for x in elements]
            #적정가격
            elements = driver.find_elements_by_css_selector('#contents > div.table_style01 > table > tbody > tr > td.text_r.txt_number') #적정가격
            list_propPrice = [x.text for x in elements]
            #투자의견
            elements = driver.find_elements_by_css_selector('#contents > div.table_style01 > table > tbody > tr > td:nth-child(4)')  #투자의견
            list_investOpinion = [x.text for x in elements]
            # 작성자
            elements = driver.find_elements_by_css_selector('#contents > div.table_style01 > table > tbody > tr > td:nth-child(5)') #작성자
            list_writer = [x.text for x in elements]
            # 제공출처
            elements = driver.find_elements_by_css_selector('#contents > div.table_style01 > table > tbody > tr > td:nth-child(6)') #제공출처
            list_provider = [x.text for x in elements]
            # 리포트 URL
            elements = driver.find_elements_by_css_selector('#contents > div.table_style01 > table > tbody > tr > td:nth-child(9) > div > a') # 리포트 URL
            list_reportURL = [x.get_property("href") for x in elements]

            print("Page No: ", pageNo)
            no = 1
            for date, title, price, opinion, writer, provider, url in zip(list_writeDate, list_title, list_propPrice, list_investOpinion, list_writer, list_provider, list_reportURL):

                print(f"{no}) {date} | {title} | {price} | {opinion} | {writer} | {provider} | {url}")
                no += 1

                # 보고서 제목에서 종목명, 종목코드 추출
                match = rPattern.match(title)
                if match == None:
                    print("종목명, 종목코드 추출 실패!")
                    continue

                stock_name, stock_code = match.groups()

                # 종목코드 추출값에 "상장예정" 과 같은 문구가 콤마 구분으로 나타나는 예외 확인 시
                # 콤마를 기준으로 해당 종목의 종목코드를 추출해준다.
                idx = stock_code.find(",")
                if idx != -1:
                    stock_code = stock_code[0:idx+1]

                # 보고서 정보
                doc_report={
                    "date": date.strip(),
                    "stock_name": stock_name.strip(),
                    "stock_code": stock_code.strip(),
                    "title": title.strip(),
                    "price": int(price.replace(",", "").strip()),
                    "opinion": opinion.strip(),
                    "writer": writer.strip(),
                    "provider": provider.strip(),
                    "url": url.strip()
                }

                if createNewClt == True: # 기존 컬렉션이 없다면
                    STOCK_REPORT_CLT.insert_one(doc_report)
                else: # 컬렉션이 존재한다면 => 중복 확인 후 입력
                    existDoc = STOCK_REPORT_CLT.find_one({"url": url})
                    if existDoc == None:
                        STOCK_REPORT_CLT.insert_one(doc_report)

        if createNewClt:
            STOCK_REPORT_CLT.create_index([("date", pymongo.DESCENDING)], unique=False)
            STOCK_REPORT_CLT.create_index([("url", pymongo.DESCENDING)], unique=True)

        driver.close()



#================================ DatabaseConstructor =====================================

dbConstruct = DatabaseConstructor()

if __name__ == "__main__":
    a = 0
    #dbConstruct.updateTodayStockPriceInfo() #오늘 종목가격 업데이트
    #dbConstruct.download_FnGuide_pages(1)  # 제무재표 페이지 다운로드
    #dbConstruct.download_FnGuide_pages(2)  # 제무비율 페이지 다운로드
    #dbConstruct.download_FnGuide_pages(3)  # snapshot 페이지 다운로드
    #dbConstruct.constructDB_financialStatement_data("E:/STOCK/download/FS/2021-06-01", 2021, 1, 'q') # 제무제표 데이터 구축
    #dbConstruct.constructDB_financialStatement_data("E:/STOCK/download/FS/2021-04-22")  # 제무제표 데이터 구축
    #dbConstruct.constructDB_financialRatio_data("E:/STOCK/download/FR/2021-06-01", 2021, 1, False, True) # 제무비율 데이터 구축
    #dbConstruct.constructDB_financialRatio_data("E:/STOCK/download/FR/2021-04-22")  # 제무비율 데이터 구축
    #dbConstruct.constructDB_consensus_data("E:/STOCK/download/CS/2021-06-01")  # 컨센서스 데이터 구축
    #dbConstruct.writeUnderSrim100InVolumeRTop200()
    #dbConstruct.writeVolumnJumpStocks(20,20)

    #dbConstruct.crawlingNaverStockNewsInfo() # 네이버 뉴스정보 크롤링 후 저장
    #dbConstruct.removeDuplicateDocInNaverNewDB() # 뉴스 DB에 중복 입력된 데이터 제거(초기 DB구축 시 오입력 수정용)

    dbConstruct.crawlingHKConsensusReportInfo("2021-05-25", "2021-06-25")