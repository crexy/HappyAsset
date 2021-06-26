from pymongo import MongoClient
from pymongo import UpdateOne
from operator import itemgetter
import csv
#from scipy import stats
from StockDB import stockDB
from scipy import stats
import datetime
import re

import pandas as pd
import numpy as np

# 한글 문자열 변수 선언
CIS = "포괄손익계산서"
BS = "재무상태표"
CS = "현금흐름표"
SALES = "매출액"
CSH = "지배주주지분"  # Controlling ShareHolder
PSHS = "지배기업주주지분"  # Parent ShareHolder's Share
TOTCAP = "자본총계"
SnM_C_Dt = "판매비와관리비_상세"
RnD_C = "연구개발비"

# 데이터 분석 관련
class DataAnalysis:

    # S-RIM 계산
    def __calc_S_RIM(self, bve:int, roe:float, k:float, w:float, cnt:int) -> int:
        '''
        :param bve: 지배주주지분 총자산
        :param roe: ROE
        :param k: 할인률
        :param w: 초과이익 지속계수
        :param cnt: 발행주식수
        :return: S-RIM 가격
        '''
        roe = roe / 100
        k = k / 100
        if roe - k >= 0:  # 잉여이익이 양수인 경우
            s_rim = (bve + bve * (roe - k) * w / (1 + k - w)) / cnt
        else:  # 잉여이익이 음수인 경우
            s_rim = ((bve + bve * (roe - k) / k) / cnt) * w
        return int(s_rim)


    # S-RIM 업데이트
    def updateAll_S_RIM(self, year:int, quarter:int) -> None:

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]
        QUARTER_FS_DATA_CLT = stockDB.FS_DB["QUARTER_FS_DATA_CLT"]

        # 최근분기 지배주주지분 총자본 정보
        list_totalAsset = QUARTER_FS_DATA_CLT.find({'year': year, 'quarter': quarter},
                                                   {'_id': 0, 'stock_code': 1, '재무상태표.지배기업주주지분': 1})

        # for x in list_totalAsset:
        #     if PSHS not in x[BS]:
        #         print(x['stock_code'])

        # 종목코드:지배주주지분 자산 map 생성
        dicTotalAsset = {x['stock_code']: x[BS][PSHS] for x in list_totalAsset if PSHS in x[BS]}

        list_corp = STOCK_CROP_DATA_CLT.find({})

        discntR = 8  # 할인율

        list_bulk = []
        rim_cnt = 1
        for doc in list_corp:
            bve = 'N/A';
            roe = 'N/A'
            stock_code = doc['stock_code']
            if 'treasury_stock' not in doc:
                continue

            issued_cnt = doc['issued_shares_num'] # 발행 주식 수
            treasury_cnt = doc['treasury_stock']    # 자기 주식 수

            if treasury_cnt == 'N/A': continue
            if 'cns_year' not in doc: continue

            print(stock_code)

            # dic_cns_year = doc['cns_year']
            # if 'ROE' in dic_cns_year:
            #     if dic_cns_year['ROE'] != 'N/A':
            #         roe = dic_cns_year['ROE']
            #     else: roe = 0
            # else:
            #     roe = 0 # 자본 잠식등으로 추정 ROE가 산출되지 못한 경우

            # 컨센서스 데이터가 존재하는 종목인 경우 년도 컨센서스의 ROE를 사용하고
            if SALES in doc['cns_year']:
                dic_cns_year = doc['cns_year']
                if 'ROE' in dic_cns_year:
                    if dic_cns_year['ROE'] != 'N/A':
                        roe = dic_cns_year['ROE']
                    else: roe = 0 # ROE가 산출도지 못한 경우
                else: roe = 0 # ROE가 산출도지 못한 경우

            else:  # 컨센서스 데이터가 존재하지 않는 종목은 년간 추정 ROE를 사용함
                dic_cns_year = doc['cns_year']
                if 'ROE' in dic_cns_year:
                    if dic_cns_year['ROE'] != 'N/A':
                        roe = dic_cns_year['ROE']
                    else:
                        roe = 0  # ROE가 산출도지 못한 경우
                else:
                    roe = 0  # ROE가 산출도지 못한 경우
                    

            shareCnt = issued_cnt - treasury_cnt
            if stock_code in dicTotalAsset:
                bve = dicTotalAsset[stock_code]  # 최근분기 지배주주지분 총자본
            else:
                dic_cns_quarter = doc['cns_quarter']
                dic_cns_year = doc['cns_year']
                if CSH in dic_cns_quarter:
                    bve = dic_cns_quarter[CSH]
                elif TOTCAP in dic_cns_quarter:
                    bve = dic_cns_quarter[TOTCAP]
                elif CSH in dic_cns_year:
                    bve = dic_cns_year[CSH]

            if (bve != 'N/A' and roe != 'N/A' and roe > 0):
                stock_name = doc['stock_name']
                # print(f'{rim_cnt}){stock_name}')
                bve *= 100000000
                val100 = self.__calc_S_RIM(bve, roe, discntR, 1, shareCnt)  # 적정가격
                val090 = self.__calc_S_RIM(bve, roe, discntR, 0.9, shareCnt)  # 10% 할인가격
                val080 = self.__calc_S_RIM(bve, roe, discntR, 0.8, shareCnt)  # 20% 할인가격
                
                # print(f'{rim_cnt}){stock_name}: {list_rim}')
                rim_cnt += 1
            else:
                val080 = 0
                val090 = 0
                val100 = 0

            list_rim = [doc['stock_code'], val100, val090, val080]
            list_bulk.append(list_rim)

        list_bulk_qry = []
        for item in list_bulk:
            list_bulk_qry.append(
                UpdateOne({'stock_code': item[0]},
                          {'$set': {'S-RIM.100': item[1], 'S-RIM.090': item[2], 'S-RIM.080': item[3]}})
            )
            if (len(list_bulk_qry) == 1000):
                STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
                print(f'{len(list_bulk_qry)})개 S-RIM 데이터 업데이트')
                list_bulk_qry = []

        if (len(list_bulk_qry) > 0):
            STOCK_CROP_DATA_CLT.bulk_write(list_bulk_qry, ordered=False)
            print(f'{len(list_bulk_qry)})개 S-RIM 데이터 업데이트')

    # 매출액 정보 얻기
    def __genSalesInfo(self, crsStockInfo, fsCLT, bYear):

        for stockInfo in crsStockInfo:
            stock_code = stockInfo["stock_code"]

            if bYear == False:
                crsFsInfo = fsCLT.find({"stock_code": stock_code}).sort([
                    ('year', 1),
                    ('quarter', 1)
                ])
            else:
                crsFsInfo = fsCLT.find({"stock_code": stock_code}).sort('year', 1)

            listSaleInfo = []  # 매출액 정보(기간 정보 포함)
            listSaleVal = []  # 매출액 선형회귀를 위한 매출액 정보
            for fsInfo in crsFsInfo:
                dic_sales = dict()  # 매출액 정보 dict
                if "매출액" in fsInfo["포괄손익계산서"]:
                    dic_sales["sales"] = fsInfo["포괄손익계산서"]["매출액"]
                elif "순이자손익" in fsInfo["포괄손익계산서"]:  # 금융계열 종목
                    dic_sales["sales"] = fsInfo["포괄손익계산서"]["순이자손익"]
                elif "영업수익" in fsInfo["포괄손익계산서"]:  # 금융계열 종목
                    dic_sales["sales"] = fsInfo["포괄손익계산서"]["영업수익"]
                else:
                    dic_sales["sales"] = 0

                dic_sales["year"] = fsInfo["year"]
                if bYear == False: dic_sales["quarter"] = fsInfo["quarter"]
                listSaleInfo.append(dic_sales)
                listSaleVal.append(dic_sales["sales"])

            if len(listSaleInfo) == 0: continue
            '''
            slope: 회귀선의 기울기입니다.
            intercept: 회귀선의 절편입니다.
            rvalue: 상관 계수.
            pvalue: 귀무 가설이 있는 가설 검정의 양측 p-값 기울기가 0인지 여부, t-분포와 함께 Wald Test를 사용합니다. 검정 통계량
            stderr: 추정된 그라데이션의 표준 오차입니다.        
            '''
            slope, intercept, r_value, p_value, stderr = stats.linregress(range(len(listSaleVal)), listSaleVal)
            dic_sales_linregress = {
                'slope': slope/intercept,
                'intercept': intercept,
                'r_value': r_value,
                'p_value': p_value,
                'stderr': stderr/intercept
            }

            div_key =  'year_sales' if bYear else 'quarter_sales' # 분기와 년도 데이터의 키값 지정
            # salesInfo > year/quarter > sale, linRegress 구조로 정보 업데이트
            dic_data={'sales': listSaleInfo, 'linRegress': dic_sales_linregress}
            yield UpdateOne({'stock_code': stock_code},
                            {'$set': {div_key: dic_data}})

    # 종목의 매출액 정보 업데이트
    def updateStockSalesInfo(self):

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]  # 종목정보 컬렉션
        QUARTER_FS_DATA_CLT = stockDB.FS_DB["QUARTER_FS_DATA_CLT"]  # 분기 제무재표 정보 컬렉션
        YEAR_FS_DATA_CLT = stockDB.FS_DB["YEAR_FS_DATA_CLT"]        # 년간 제무재표 정보 컬렉션

        # 종목의 분기, 년간 제무제표 중 매출액 정보를 얻어 종복정보에 업데이트 해준다.

        crsStockInfo = STOCK_CROP_DATA_CLT.find({})
        # 분기 제무제표 데이터의 매출액 정보 업데이트 벌크 쿼리 받기
        geneQuarterQryList = self.__genSalesInfo(crsStockInfo, QUARTER_FS_DATA_CLT, False)

        crsStockInfo = STOCK_CROP_DATA_CLT.find({})
        geneYearQryList = self.__genSalesInfo(crsStockInfo, YEAR_FS_DATA_CLT, True)

        # 분기/년도 매출액 데이터 벌크 업데이트 수행
        STOCK_CROP_DATA_CLT.bulk_write(list(geneQuarterQryList), ordered=False)

        STOCK_CROP_DATA_CLT.bulk_write(list(geneYearQryList), ordered=False)

        #db.getCollection('STOCK_CROP_DATA_CLT').updateMany({}, { $unset: {salesInfo: 1}});


    def getSaleGrowthStock(self):
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]  # 종목정보 컬렉션

        curs = STOCK_CROP_DATA_CLT.find({}).sort('year_sales.linRegress.slope', -1)

        no = 0
        for i, stock in enumerate(curs):
            slope = stock["year_sales"]["linRegress"]["slope"]
            stderr = stock["year_sales"]["linRegress"]["stderr"]
            if stderr > 0.5: continue
            print(f"{stock['stock_name']}({stock['stock_code']}): {slope}")
            no += 1
            if no > 10 : break


    # 종목필터링 컨셉
    # 1. 20년 매출액, 영업이익 21년 컨센 매출액, 영업이익 증가율 계산 => 매출액 증가률 top 30, 영업이익 증가율 top30

    # 컨센서스 성장 증가율 TOP 30 종목
    def concensusTop30SalesGrowthRateStocks(self, year, saveFilepath):
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]  # 종목정보 컬렉션
        YEAR_FS_DATA_CLT = stockDB.FS_DB["YEAR_FS_DATA_CLT"]  # 년간 제무재표 정보 컬렉션

        #컨센서스가 있는 종목만 대상으로 선정
        corp_curs = STOCK_CROP_DATA_CLT.find({'cns_year.매출액': {'$exists': True}})

        # corp_curs = STOCK_CROP_DATA_CLT.find({"$or":[{'cns_year.매출액': {'$exists': True}},\
        #                                             {'cns_year.이자수익': {'$exists': True}},\
        #                                             #{'cns_year.보험료수익': {'$exists': True}},\
        #                                             {'cns_year.순영업수익': {'$exists': True}}]})
        # 컨센서스 매출액 등 영업수익 항목명칭 조사
        # firstItem = set()
        # for i, corp in enumerate(listCorp):
        #     print((i+1), corp["stock_name"])
        #     keys = corp["cns_year"].keys()
        #     firstItem.add(list(keys)[0])
        # for item in firstItem:
        #     print(item)

        list_stock_codes = list()
        dic_cns={} # 컨센서스가 존재하는 종목정보
        for corp in corp_curs:
            stock_code = corp["stock_code"]
            list_stock_codes.append(stock_code)
            dic_cns[stock_code] = corp

        fs_curs = YEAR_FS_DATA_CLT.find({'year':year, 'stock_code':{'$in':list_stock_codes}})
        dic_fs = {x['stock_code']:x for x in fs_curs} # 컨센서스가 존재하는 종목의 제무재표 정보

        #listSalesName = ["매출액", "이자수익", "보험료수익", "순영업수익"]
        listSalesName = ["매출액"]

        # 컬럼 리스트
        listColumns = ['매출액증가율', '영업이익증가율', '비고', '종목명', '현재가', '시가총액', 'PER', 'ROE', '매출액', '영업이익', '이자보상비율', 'S-RIM80', 'S-RIM90', 'S-RIM100', 'S-RIM80 R']
        dfGrowth = pd.DataFrame(columns=listColumns)
        dfGrowth.index.name = "종목코드"

        for stock_code in dic_cns.keys():
            cropData = dic_cns[stock_code]
            cnsData = cropData['cns_year']  # 컨센서스 데이터

            if stock_code not in dic_fs: continue
            fsData = dic_fs[stock_code]['포괄손익계산서'] # 제무제표 데이터
            for sname in listSalesName: # 매출명칭
                if sname in cnsData:
                    lastSales = fsData[sname]    #매출액 데이터
                    if lastSales == 0: continue
                    cnsSales = cnsData[sname]

                    lastOperProfit = fsData['영업이익']  #영업이익 데이터
                    cnsOperProfit = cnsData['영업이익']

                    # 매출액증가율
                    salesGrw = (cnsSales-lastSales)/lastSales*100

                    # 영업이익 증가율
                    operProfitGrw = (cnsOperProfit-lastOperProfit)/lastOperProfit*100

                    # 비고
                    remark = ""
                    if lastOperProfit < 0 and cnsOperProfit > 0: remark = "흑전"
                    elif lastOperProfit > 0 and cnsOperProfit > 0: remark = "흑지"
                    elif lastOperProfit < 0 and cnsOperProfit < 0: remark = "적지"
                    elif lastOperProfit > 0 and cnsOperProfit < 0: remark = "적전"

                    if lastOperProfit < 0:
                        operProfitGrw *= -1

                    stock_name = cropData["stock_name"]  # 종목명
                    cur_price = cropData['cur_price']  # 현재가
                    issued_shares_num = cropData['issued_shares_num']  # 발행주식수
                    market_capitalization = cur_price * issued_shares_num /100000000  # 시가총액
                    PER = cropData["PER"]
                    ROE = cropData["ROE"]
                    sales = cropData["매출액"]
                    operating_profit = cropData["영업이익"]
                    ICR = cropData["이자보상비율"]  # Interest Coverage Ratio
                    # S-RIM 가격
                    SRIM80 = cropData["S-RIM"]["080"]
                    SRIM90 = cropData["S-RIM"]["090"]
                    SRIM100 = cropData["S-RIM"]["100"]
                    SRIM80_R = 0  # S-RIM80 가격 대비 현가격 비율
                    if SRIM80 != 0:
                        SRIM80_R = cur_price / SRIM80 * 100

                    # ['매출액증가율', '영업이익증가율', '비고', '종목명', '현재가', '시가총액', 'PER', 'ROE', '매출액', '영업이익', '이자보상비율', 'S-RIM80', 'S-RIM90', 'S-RIM100', 'S-RIM80 R']

                    dfGrowth.loc[stock_code] = [salesGrw, operProfitGrw, remark, stock_name, cur_price, market_capitalization, PER, ROE, sales, operating_profit, ICR, SRIM80, SRIM90, SRIM100, SRIM80_R]

        #print(dfGrowth.columns)
        # 매출액증가율로 정렬
        dfSalesSort = dfGrowth.sort_values(by=['매출액증가율'], axis=0, ascending=False)
        # 영업이익증가율로 정렬
        dfOperProfitSort = dfGrowth.sort_values(by=['영업이익증가율'], axis=0, ascending=False)

        setSalesTop70 = set(dfSalesSort.index[:70])
        setOprFtTop70 = set(dfOperProfitSort.index[:70])

        setInter = setSalesTop70 & setOprFtTop70 # 매출액증가율 상위 30, 영업이익 증가율 상위 30 교집합

        dfGrowthInter = pd.DataFrame(columns=listColumns)
        for stock_code in setInter:
            dfGrowthInter.loc[stock_code] = dfSalesSort.loc[stock_code]

        listDelIdx1=[]
        listDelIdx2=[]
        for i in range(len(dfSalesSort.index)):
            if i >= 30:
                listDelIdx1.append(dfSalesSort.index[i])
                listDelIdx2.append(dfOperProfitSort.index[i])
        dfSales_IR_TOP30 = dfSalesSort.drop(listDelIdx1)    # 매출액 증가율 TOP30
        dfOperProfit_IR_TOP30 = dfOperProfitSort.drop(listDelIdx2) # 영업이익 증가율 TOP30

        dfGrowthInter.to_csv(saveFilepath+"\\CNS_매출액영업이익.csv", encoding="UTF-8")
        dfSales_IR_TOP30.to_csv(saveFilepath+"\\CNS_매출액_TOP30.csv", encoding="UTF-8")
        dfOperProfit_IR_TOP30.to_csv(saveFilepath+"\\CNS_영업이익_TOP30.csv", encoding="UTF-8")

    # 유동주식 대비 거래량 순위 종목 기간 수익률 구하기
    def getVolumnRateYield(self, _prevDays, _startDate, _endDate, _reslut_day_cnt, _trgVolPer, targetEarningR):

        def getPriceDataFrame(cursCorp):
            no = 1
            for corp in cursCorp:

                stock_code = corp["stock_code"]
                stock_name = corp["stock_name"]
                if "S-RIM" in corp:
                    srim100 = corp["S-RIM"]["100"]
                else:
                    srim100 = 0

                strStartDate = str(_startDate)
                dt_start = datetime.datetime.strptime(strStartDate, "%Y%m%d")
                dt_start = dt_start + datetime.timedelta(days=-_prevDays)
                strStartDate = dt_start.strftime("%Y%m%d")
                startDate = int(strStartDate)

                filter ={"$and":
                    [{"날짜":{"$gte":startDate}},
                     {"날짜":{"$lte":_endDate}}]}

                doc_count = stockDB.SP_DB["A" + stock_code].count_documents(filter)

                if doc_count == 0: continue

                cursPrice = stockDB.SP_DB["A"+stock_code].find(filter)

                df = pd.DataFrame(list(cursPrice))

                print(f"{no}){stock_name} 데이터 획득")
                no+=1

                yield {"stock_code":stock_code,
                       "stock_name":stock_name,
                       "srim100":srim100,
                       "df":df}

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]  # 종목정보 컬렉션

        cursCorp = STOCK_CROP_DATA_CLT.find({})

        gen_priceInf_df = getPriceDataFrame(cursCorp)

        dfVolumn = pd.DataFrame()
        dfNextPriceR = pd.DataFrame()

        dicStockName = {}
        dicSrim={}
        for dic_priceInf_df in gen_priceInf_df:
            stock_code = dic_priceInf_df["stock_code"]
            stock_name = dic_priceInf_df["stock_name"]
            srim100 =  dic_priceInf_df["srim100"]
            dicStockName[stock_code] = stock_name
            dicSrim[stock_code] = srim100
            dfRaw = dic_priceInf_df["df"]

            # 종목의 일별 거래량 비율을 구해서 DataFrame에 저장
            ltVolumnR=[]
            ltDate=[]
            ltNextDayPriceR=[]

            rowCnt = dfRaw.shape[0]
            for i in range(rowCnt):
                volumn = dfRaw["거래량"][i]                
                ltVolumnR.append(volumn)
                date = dfRaw["날짜"][i]
                ltDate.append(date)
                if i < rowCnt-1:
                    nextDayPriceR = (dfRaw["종가"][i+1]-dfRaw["종가"][i])/dfRaw["종가"][i]*100
                else:
                    nextDayPriceR = 0
                ltNextDayPriceR.append(nextDayPriceR)

            seriesVolumn = pd.Series(ltVolumnR, index=ltDate)
            dfVolumn[stock_code] = seriesVolumn

            seriesDayPriceR = pd.Series(ltNextDayPriceR, index=ltDate)
            dfNextPriceR[stock_code] = seriesDayPriceR

        # 아래 형태의 데이터 프래임 => dfVolumnR
        #      | 종목1 | 종목2 | 종목3 | ...
        # 날짜1 | 11.3 | 22.3 | 22.5 |
        # 날짜2 | 11.3 | 22.3 | 22.5 |
        # 날짜3 | 11.3 | 22.3 | 22.5 |

        ltPickStock=[]
        ltStockCode = dfVolumn.columns
        no = 1
        for stock_code in ltStockCode:
            seriesR = dfVolumn[stock_code]

            idxDate = seriesR[seriesR.index >= _startDate].index
            
            for idate in idxDate:
                #idate = int(idate)
                prevSeries = seriesR[seriesR.index < idate]
                if prevSeries.shape[0] > _reslut_day_cnt: # 이전 평균값을 지정한 기간 범위 안에서 계산하기 위해
                    prevSeries = prevSeries.drop(prevSeries.index[0])
                volumnMean = prevSeries.mean()

                if volumnMean < 1000:  # 평균 거래량이 1000 밑 종목은 거랴정지 가능성이 있음
                    break

                volumn = seriesR[idate]

                if volumn > volumnMean * _trgVolPer: # 거래량이 평균대비 X배 높은 종목를 찾는다.
                    # if dicStockName[stock_code] == '세화피앤씨':
                    #     a = 0
                    if dfNextPriceR[stock_code][idate] < 2: break # 거래량이 터진 다음날 주가가 떨어지면 Skip
                    dicPick={
                        "stock_code":stock_code,
                        "baseDate":int(idate),
                        "volumnMean":volumnMean,
                        "trgVolumn":volumn}
                    ltPickStock.append(dicPick)

                    print(f"{no}){dicStockName[stock_code]} PICK")
                    no += 1
                    break

        dfResult = pd.DataFrame()

        ltStockCode =[]
        ltStockName=[]
        ltBaseDate=[]
        ltmaxEarningR=[]
        ltDaysTerm=[]
        ltTrgVolumR=[]
        ltSRIMR=[]

        fmt2f = lambda x: format(x, '.2f')

        no = 1
        for dicPick in ltPickStock:
            stock_code = dicPick["stock_code"]
            baseDate = dicPick["baseDate"]

            cursPrice = stockDB.SP_DB["A" + stock_code].find({"날짜": {"$gte": int(baseDate)}}).limit(_reslut_day_cnt)
            stock_name = dicStockName[stock_code]
            basePrice = 0
            market_cap = 0
            ltEarningR =[]
            ltDate=[]

            skipStock = False

            for i, priceInf in enumerate(cursPrice):
                if basePrice == 0:
                    basePrice = priceInf["종가"]
                    highPrice = priceInf["고가"]
                    startPrice = priceInf["시가"]
                    tbDiffWidth = (basePrice - highPrice ) / highPrice * 100
                    if startPrice > basePrice or tbDiffWidth <= -7:
                        skipStock = True
                        break
                    market_cap = priceInf["시가총액"]
                    continue

                if i == 1:
                    endPrice = priceInf["종가"]
                    highPrice = priceInf["고가"]
                    startPrice = priceInf["시가"]
                    tbDiffWidth = (endPrice - highPrice ) / highPrice * 100
                    if startPrice > endPrice and tbDiffWidth <= -5:
                        skipStock = True
                        break

                if i == 2:
                    endPrice = priceInf["종가"]
                    if endPrice < basePrice:
                        skipStock = True
                        break

                earningR = (priceInf['종가'] - basePrice)/basePrice*100
                sdate = priceInf['날짜']
                #print(f"{sdate}) {format(earningR,'.2f')}")
                ltEarningR.append(earningR)
                ltDate.append(sdate)

            if skipStock : continue

            seriesRslt = pd.Series(ltEarningR, index=ltDate)
            pickDate = seriesRslt.idxmax()
            maxEarningR = seriesRslt.max()

            dt_s = datetime.datetime.strptime(str(baseDate), "%Y%m%d")
            dt_p = datetime.datetime.strptime(str(pickDate), "%Y%m%d")

            dayTerm = (dt_p-dt_s).days

            #if maxEarningR < 50:continue

            trigerVolumnR = dicPick["trgVolumn"] / dicPick["volumnMean"]

            if trigerVolumnR == np.inf:
                a = 0

            print(f"★{no}) {stock_name} 기준일: {baseDate} 시가총액: {market_cap / 100000000}억")
            no += 1
            print(f"최대수익률:{fmt2f(maxEarningR)}({pickDate}), 경과일:{dayTerm}, 트리거 거래비율:{fmt2f(trigerVolumnR)}배")
            print("====================================================")
            print("")

            ltStockCode.append(stock_code)
            ltStockName.append(stock_name)
            ltBaseDate.append(baseDate)
            ltmaxEarningR.append(maxEarningR)
            ltDaysTerm.append(dayTerm)
            ltTrgVolumR.append(trigerVolumnR)

            srim100 = dicSrim[stock_code]
            if srim100 == 0:
                srimR = np.nan
            else:
                srimR = basePrice/srim100*100
            ltSRIMR.append(srimR)

        if len(ltStockCode) == 0 :
            print("=========== 해당 종목이 없음! =========== ")
            return

        dfResult["stock_code"] = ltStockCode
        dfResult["stock_name"] = ltStockName
        dfResult["baseDate"] = ltBaseDate
        dfResult["maxEarningR"] = ltmaxEarningR
        dfResult["dayTerm"] = ltDaysTerm
        dfResult["trigerVolumnR"] = ltTrgVolumR
        dfResult["srimR"] = ltSRIMR

        underTargetEarningRCnt = dfResult["maxEarningR"][dfResult["maxEarningR"] < targetEarningR].shape[0]
        totCnt = dfResult.shape[0]
        overTargetEarningR = 100 - (underTargetEarningRCnt / totCnt * 100)

        print("==============================================================")
        print("전체 정보")
        print("수익률 평균:"+fmt2f(dfResult["maxEarningR"].mean()))
        print("수익률 Std:" + fmt2f(dfResult["maxEarningR"].std()))
        print("트리거 거래량 배율 평균:" + fmt2f(dfResult["trigerVolumnR"].mean()))
        print("트리거 거래량 배율 Std:" + fmt2f(dfResult["trigerVolumnR"].std()))
        print("SRIM 평균 비율:" + fmt2f(dfResult["srimR"].mean(skipna=True)))
        print("SRIM Std:" + fmt2f(dfResult["srimR"].std(skipna=True)))
        print("==============================================================")
        print("")
        print(f"{targetEarningR}% 미만 수익비율: {fmt2f(100-overTargetEarningR)}")
        dflessTargetEarningR = dfResult[dfResult["maxEarningR"] < targetEarningR]
        print("수익률 평균:" + fmt2f(dflessTargetEarningR["maxEarningR"].mean()))
        print("수익률 Std:" + fmt2f(dflessTargetEarningR["maxEarningR"].std()))
        print("트리거 거래량 배율 평균:" + fmt2f(dflessTargetEarningR["trigerVolumnR"].mean()))
        print("트리거 거래량 배율 표준편차" + fmt2f(dflessTargetEarningR["trigerVolumnR"].std()))
        print("SRIM 평균 비율:" + fmt2f(dflessTargetEarningR["srimR"].mean(skipna=True)))
        print("SRIM Std:" + fmt2f(dflessTargetEarningR["srimR"].std(skipna=True)))
        print("==============================================================")
        print("")
        print(f"{targetEarningR}% 초과 수익비율: {fmt2f(overTargetEarningR)}")
        dfoverTargetEarningR = dfResult[dfResult["maxEarningR"] > targetEarningR]
        print("수익률 평균:" + fmt2f(dfoverTargetEarningR["maxEarningR"].mean()))
        print("수익률 Std:" + fmt2f(dfoverTargetEarningR["maxEarningR"].std()))
        print("트리거 거래량 배율 평균:"+ fmt2f(dfoverTargetEarningR["trigerVolumnR"].mean()))
        print("트리거 거래량 배율 표준편차"+fmt2f(dfoverTargetEarningR["trigerVolumnR"].std()))
        print("SRIM 평균 비율:" + fmt2f(dfoverTargetEarningR["srimR"].mean(skipna=True)))
        print("SRIM Std:" + fmt2f(dfoverTargetEarningR["srimR"].std(skipna=True)))















        # vRowCnt = dfVolumnR.shape[0]
        #
        # for i in range(vRowCnt):
        #     idxDate = dfVolumnR.index[i]
        #     series = dfVolumnR.loc[idxDate].sort_values(ascending=False)
        #
        #     # 설정된 거래량 비율 구간: 퍼센테이지 ex) 20% ~ 30%
        #     # 안의 종목을 구하기
        #     series1 = series >= startP
        #     series2 = series <= endP
        #     series1 = series1 & series2
        #     series1 = series[series1]
        #
        #     sum_earnings_r = 0
        #     plusCnt = 0
        #     for j in range(series1.size):
        #         stock_code = series1.index[j]
        #
        #         cursPrice = stockDB.SP_DB["A" + stock_code].find({"날짜": {"$gte": int(idxDate)}}).limit(reslut_day_cnt)
        #         ltPrice = []
        #         for priceInf in cursPrice:
        #             ltPrice.append(priceInf["종가"])
        #
        #         startPrice = ltPrice[0]
        #         lastPrice = ltPrice[-1]
        #
        #         earnings_r = (lastPrice - startPrice) / startPrice * 100
        #         print(idxDate, 1+j, docStockName[stock_code], earnings_r)
        #
        #         if earnings_r > 0:
        #             plusCnt += 1
        #
        #         sum_earnings_r += earnings_r;
        #     print("수익률 합: ", sum_earnings_r,"%")


























#================================ DataAnalysis =====================================

dataAnalysis = DataAnalysis()

if __name__ == "__main__":
    a = 0
    dataAnalysis.updateAll_S_RIM(2021, 1) #S-RIM 가격 정보 Update
    #dataAnalysis.updateStockSalesInfo() # 종목 매출액(시계열) 정보 업데이트
    #dataAnalysis.getSaleGrowthStock()
    #dataAnalysis.concensusTop30SalesGrowthRateStocks(2020, "C:\\STOCK_DATA")

    # for i in range(200):
    #     v_r = dataAnalysis.getVolumnRateYield(i, 20210102, 20210201, 20)
    #     print(i+1,"rank : ",v_r, "%")


    #dataAnalysis.getVolumnRateYield(20, 20210101, 20210201, 60, 20, 100)
    #dataAnalysis.getVolumnRateYield(20, 20201201, 20210101, 50, 20, 100)
    #dataAnalysis.getVolumnRateYield(20, 20201101, 20201201, 60, 20, 100)

