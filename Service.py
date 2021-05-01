from StockDB import stockDB
from DataAnalysis import dataAnalysis
import re

class service:

    # 종목정보 리스트 조회
    def searchStockInfoList(self, stock_keyword=None, business_keyword=None, customer_keyword=None, product_keyword=None):
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

        if business_keyword != None:
            if len(business_keyword) >= 2:
                rgxBusiness = re.compile(f'.*{business_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'business': rgxBusiness})

        if customer_keyword != None:
            if len(customer_keyword) >= 2:
                rgxCustomer = re.compile(f'.*{customer_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'customer': rgxCustomer})

        if product_keyword != None:
            if len(product_keyword) > 2:
                rgxProduct = re.compile(f'.*{product_keyword}.*', re.IGNORECASE)  # compile the regex
                listWhereCondi.append({'product': rgxProduct})

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
                                      'business': stockInfo['business']}
                             })
        return rslt.modified_count


haService = service()
