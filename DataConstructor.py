import csv
from time import sleep
from os.path import isfile, join
from os import listdir

from StockInvestDB import stockDB
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

from datetime import datetime


# 데이터 베이스 구축 관련 기능

class DatabaseConstructor:

    # 오늘의 전종목 시세 파일 다운로드 후 DB 업데이트
    def updateTodayStockPriceInfo(self): #date 포멧ex) 20210327
        self.__downloadTodayStockPriceInfoFile()

        # 다운로드 폴더에서 다운된 전종목 시세 파일 경로 확인
        downloadPath = 'C:\\Users\\박혜미\\Downloads'

        files = [join(downloadPath, f) for f in listdir(downloadPath) if isfile(join(downloadPath, f)) and ".csv" in f]

        today = datetime.today().strftime('%Y%m%d')
        
        # 현재 가격 정보 업데이트
        for filepath in files:
            if today in filepath:
                self.__update_stock_corp_data(filepath, today)
                break

        print('종목 현재가 업데이트 완료!')

    # KRX 정보시스템에서 오늘의 전정목 시세 정보 파일 다운로드
    def __downloadTodayStockPriceInfoFile(self, dateList=None):
        options = webdriver.ChromeOptions()
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

        STOCK_CROP_DATA_CLT = stockDB.database["STOCK_CROP_DATA_CLT"]
        YM_ASSET_STOCK_CTL = stockDB.database["YM_ASSET_STOCK_CLT"]   #보유종목 컬렉션

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

dbConstruct = DatabaseConstructor()

if __name__ == "__main__":
    dbConstruct.updateTodayStockPriceInfo()