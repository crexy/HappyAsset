from StockDB import stockDB
from DataAnalysis import dataAnalysis
import re

import datetime
import numpy as np
from scipy import stats

class service:
    # 종목정보 리스트 조회
    def searchStockInfoList(self, searchOpt):

        stock_keyword = None
        holdingCompany_keyword = None
        business_keyword = None
        customer_keyword = None
        product_keyword = None
        subsidiaryCompany_keyword = None

        if searchOpt != None:
            if "stock_keyword" in searchOpt:
                stock_keyword = searchOpt["stock_keyword"]
            if "holdingCompany_keyword" in searchOpt:
                holdingCompany_keyword = searchOpt["holdingCompany_keyword"]
            if "business_keyword" in searchOpt:
                business_keyword = searchOpt["business_keyword"]
            if "customer_keyword" in searchOpt:
                customer_keyword = searchOpt["customer_keyword"]
            if "product_keyword" in searchOpt:
                product_keyword = searchOpt["product_keyword"]
            if "subsidiaryCompany_keyword" in searchOpt:
                subsidiaryCompany_keyword = searchOpt["subsidiaryCompany_keyword"]

        # S-RIM 값 조회 쿼리
        CROP_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]  # 종목정보 컬렉션(테이블)

        # rsltList = CROP_CLT.find({{'$or':[{'stock_code':rgx},{'stock_name':rgx}]},
        #               {'_id':0, 'stock_code':1, 'stock_name':1, 'cur_price':1, 'S-RIM.080':1, 'S-RIM.090':1, 'S-RIM.100':1,}})

        listWhereCondi = []
        if stock_keyword != None:  # 검색어가 있을 경우
            if len(stock_keyword) >= 2:
                rgxStock = re.compile(f'.*{stock_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'stock_code': rgxStock})
                listWhereCondi.append({'stock_name': rgxStock})

        if holdingCompany_keyword != None:
            if len(holdingCompany_keyword) >= 2:
                rgxHoldingCompany_keyword = re.compile(f'.*{holdingCompany_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'holdingCompany': rgxHoldingCompany_keyword})

        if business_keyword != None:
            if len(business_keyword) >= 2:
                rgxBusiness = re.compile(f'.*{business_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'business': rgxBusiness})

        if customer_keyword != None:
            if len(customer_keyword) >= 2:
                rgxCustomer = re.compile(f'.*{customer_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'customer': rgxCustomer})

        if product_keyword != None:
            if len(product_keyword) >= 2:
                rgxProduct = re.compile(f'.*{product_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'product': rgxProduct})

        if subsidiaryCompany_keyword != None:
            if len(subsidiaryCompany_keyword) > 2:
                rgxSubsidiaryCompany = re.compile(f'.*{subsidiaryCompany_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'subsidiaryCompany': rgxSubsidiaryCompany})

        listRslt = []

        # 백분율
        ld_ratio100 = lambda x,y: x/y*100
        # 등락률 구하기
        ld_diffRation = lambda x,y: (x-y)/y*100

        totCnt = 0
        # 종목 데이터 조회
        if len(listWhereCondi) > 0:
            #crsCrop = CROP_CLT.find({'$or': listWhereCondi}).skip((page-1)*limit).limit(limit)
            crsCrop = CROP_CLT.find({'$or': listWhereCondi})
            totCnt = CROP_CLT.count_documents({'$or': listWhereCondi})

        else:
            #crsCrop = CROP_CLT.find({}).skip((page-1)*limit).limit(limit)
            crsCrop = CROP_CLT.find({})
            totCnt = CROP_CLT.count_documents({})

        # 종목별 뉴스 개수 정보 표시를 위해 현재일 기준 2일 날짜를 구함
        current = datetime.datetime.now()
        twodaysAgo = (current - datetime.timedelta(days=2)).strftime("%Y.%m.%d")

        for docStock in crsCrop:  # 종목정보

            stock_code = docStock['stock_code']
            cur_price = docStock['cur_price']

            dictRslt = {}  # 결과 데이터
            dictRslt['stock_code'] = stock_code
            dictRslt['stock_name'] = docStock['stock_name']
            dictRslt['market'] = docStock['market']
            dictRslt['cur_price'] = cur_price

            dictRslt['srim80'] = 0
            dictRslt['srim90'] = 0
            dictRslt['srim100'] = 0

            dictRslt['srim80_PR'] = 0
            dictRslt['srim90_PR'] = 0
            dictRslt['srim100_PR'] = 0

            if 'S-RIM' in docStock:
                dictRslt['srim80'] = docStock['S-RIM']['080']
                dictRslt['srim90'] = docStock['S-RIM']['090']
                dictRslt['srim100'] = docStock['S-RIM']['100']

                if dictRslt['srim80'] != 0:
                    dictRslt['srim80_PR'] = ld_ratio100(cur_price, dictRslt['srim80'])
                if dictRslt['srim90'] != 0:
                    dictRslt['srim90_PR'] = ld_ratio100(cur_price, dictRslt['srim90'])
                if dictRslt['srim100'] != 0:
                    dictRslt['srim100_PR'] = ld_ratio100(cur_price, dictRslt['srim100'])

            dictRslt['sales'] = docStock["매출액"]  # 매출액
            dictRslt['operating_profit'] = docStock["영업이익"]  # 영업이익
            dictRslt['net_profit'] = docStock["당기순이익"]  # 순이익
            dictRslt['PER'] = docStock["PER"]  # PER
            dictRslt['ROE'] = docStock["ROE"]  # ROE

            if "floatStocks" in docStock: #유동주식 정보가 있다면
                dictRslt['floatStocks'] = docStock["floatStocks"] # 유동주식수
            else:   # 유동주식 정보가 없다면 전체주식 대입
                dictRslt['floatStocks'] = docStock["issued_shares_num"]  # 전체주식수

            dictRslt['market_cap'] = 0  # 시가총액
            dictRslt['last_price'] = 0  # 전일가
            dictRslt['price_DR'] = 0 # 가격 등락률
            dictRslt['cur_volumn'] = 0  # 거래량
            dictRslt['last_volumn'] = 0  # 전일 거래량
            dictRslt['volumn_DR'] = 0 #거래량 등락률
            dictRslt['volumn_TR'] = 0 #유동주식 대비 거래량 비율

            dictRslt['market_cap'] = round(cur_price*docStock["issued_shares_num"] / 100000000)  # 시가총액 억단위로 환산
            dictRslt['last_price'] = docStock["전일종가"]  # 전일가
            dictRslt['cur_volumn'] = docStock["거래량"]  # 거래량
            dictRslt['last_volumn'] = docStock["전일거래량"]  # 전일 거래량

            dictRslt['price_DR'] = ld_diffRation(cur_price, dictRslt['last_price'])  # 가격 등락률
            if dictRslt['last_volumn'] != 0:
                dictRslt['volumn_DR'] = ld_diffRation(dictRslt['cur_volumn'], dictRslt['last_volumn'])  # 거래량 등락률
            if dictRslt['floatStocks'] != 0:
                dictRslt['volumn_TR'] = ld_ratio100(dictRslt['cur_volumn'], dictRslt['floatStocks'])  # 유동주식 대비 거래량 비율

            if "business" in docStock:
                dictRslt['business'] = docStock['business']  # 사업정보
            else:
                dictRslt['business'] = ""
            if "customer" in docStock:
                dictRslt["customer"] = docStock["customer"]  # 고객 고객사
            else:
                dictRslt["customer"] = ""
            if "holdingCompany" in docStock:
                dictRslt["holdingCompany"] = docStock["holdingCompany"]  # 지주사
            else:
                dictRslt["holdingCompany"] = ""
            if "product" in docStock:
                dictRslt["product"] = docStock["product"]  # 제품 서비스
            else:
                dictRslt["product"] = ""
            if "subsidiaryCompany" in docStock:
                dictRslt["subsidiaryCompany"] = docStock["subsidiaryCompany"]  # 종속/관계 회사
            else:
                dictRslt["subsidiaryCompany"] = ""

            # 종목별 최근 뉴스 개수
            STOCK_NEWS_CLT = stockDB.NEWS_DB["A"+stock_code] #종목 뉴스정보 컬랙션
            newsCnt = STOCK_NEWS_CLT.count_documents({"date":{"$gt":twodaysAgo}})
            dictRslt["news_count"] = newsCnt

            # 가격 변동 정보 얻기
            # 5일전, 20일전, 60일전, 120일전, 240일전 가격 정보 리스트로 전달
            STOCK_TRADE_CLT = stockDB.SP_DB["A" + stock_code]  # 종목 거래정보 컬랙션
            tcursor = STOCK_TRADE_CLT.find({},{"_id":0, "날짜":1, "종가":1}).sort("날짜",-1).skip(1).limit(240)
            no = 0
            list_priceRatio = [] # 현재가 대비 주가비율
            list_priceSlope =[] # 가격추이
            list_price = [cur_price]
            price_sum = 0

            for docPrice in tcursor:
                price = docPrice["종가"]
                price_sum += price
                list_price.append(price)
                no += 1
                # 5일(1주), 20일(한달), 60일(분기).. 정보
                if no == 5 or no == 20 or no == 60 or no == 120 or no == 240:
                    priceRatio = (cur_price - price) / price * 100
                    list_priceRatio.append(priceRatio)

                    if no == 5 or no == 20: # 이전 5일 20일 가격 변동 추이(선형회귀 기울기)
                        '''
                        slope: 회귀선의 기울기입니다.
                        intercept: 회귀선의 절편입니다.
                        rvalue: 상관 계수.
                        pvalue: 귀무 가설이 있는 가설 검정의 양측 p-값 기울기가 0인지 여부, t-분포와 함께 Wald Test를 사용합니다. 검정 통계량
                        stderr: 추정된 그라데이션의 표준 오차입니다.        
                        '''
                        list_rPrice = list(reversed(list_price))
                        slope, intercept, r_value, p_value, stderr = stats.linregress(range(len(list_rPrice)), list_rPrice)
                        list_priceSlope.append(slope/cur_price*100)

            # 가격 시계열 정보
            dictRslt["prev_5dayPrice"] = list_priceRatio[0] if len(list_priceRatio) > 0 else 0
            dictRslt["prev_20dayPrice"] = list_priceRatio[1] if len(list_priceRatio) > 1 else 0
            dictRslt["prev_60dayPrice"] = list_priceRatio[2] if len(list_priceRatio) > 2 else 0
            dictRslt["prev_120dayPrice"] = list_priceRatio[3] if len(list_priceRatio) > 3 else 0
            dictRslt["prev_240dayPrice"] = list_priceRatio[4] if len(list_priceRatio) > 4 else 0
            dictRslt["price_movingAvg"] = list_priceRatio    # 가격 이동평균 정보
            
            # 5일 20일 가격 변동 추이
            dictRslt["slope_5dayPrice"] = list_priceSlope[0] if len(list_priceSlope) > 0 else '-'
            dictRslt["slope_20dayPrice"] = list_priceSlope[1] if len(list_priceSlope) > 1 else '-'


            # 현재 종목의 일일거래정보 컬렉션이 있다면
            # if ("A" + stock_code) in stockDB.SP_DB.list_collection_names():
            #     docPrices = stockDB.SP_DB["A" + stock_code].find({}).sort("날짜", -1).limit(2)  # 가격정보 sort('year', 1)
            #
            #     dictRslt['market_cap'] = round( docPrices[0]["시가총액"] / 100000000 ) # 시가총액 억단위로 환산
            #     dictRslt['last_price'] = docPrices[1]["종가"]  # 전일가
            #     dictRslt['cur_volumn'] = docPrices[0]["거래량"]  # 거래량
            #     dictRslt['last_volumn'] = docPrices[1]["거래량"]  # 전일 거래량
            #
            #     dictRslt['price_DR'] = ld_diffRation(cur_price, dictRslt['last_price'])  # 가격 등락률
            #     if dictRslt['last_volumn'] != 0:
            #         dictRslt['volumn_DR'] = ld_diffRation(dictRslt['cur_volumn'], dictRslt['last_volumn'])  # 거래량 등락률
            #     if dictRslt['floatStocks'] != 0:
            #         dictRslt['volumn_TR'] = ld_ratio100(dictRslt['cur_volumn'], dictRslt['floatStocks'])  # 유동주식 대비 거래량 비율

            listRslt.append(dictRslt)

        return listRslt, totCnt

    def modifyStockBusinessInfo(self, stockInfo):
        CROP_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]  # 종목정보 컬렉션(테이블)
        rslt = CROP_CLT.update_one({'stock_code':stockInfo["stock_code"]},
                            {'$set': {'holdingCompany': stockInfo['holdingCompany'],
                                      'customer': stockInfo['customer'],
                                      'product': stockInfo['product'],
                                      'business': stockInfo['business'],
                                      'subsidiaryCompany':stockInfo['subsidiaryCompany']}
                             })
        return rslt.modified_count


haService = service()
