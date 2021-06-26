import pandas as pd
import pymongo
import datetime
import os
from scipy import stats

from StockDB import stockDB

# 보유종목 정보
class HoldStockInfo:
    def __init__(self, stock_code, stock_name, buying_price, stock_cnt, buying_date, price_ll_slope, price_avg, price_std):
        '''
        :param stock_code: 종목코드
        :param stock_name: 종목명
        :param buying_price: 매수가격
        :param stock_cnt: 종목수
        :param buying_date: 매수일
        '''
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.buying_price = buying_price
        self.stock_cnt = stock_cnt
        self.buying_date = buying_date
        self.cur_price = 0 # 현재가격
        self.estimated_amount = 0 # 평가자산
        self.price_ll_slope = price_ll_slope # 구매시 가격 선형회귀 기울기
        self.price_avg = price_avg # 구매시 가격 평균
        self.price_std = price_std # 구매시 가격 표준편차
        
        self.high_price = buying_price # 매수 후 최고가
        self.low_price = buying_price # 매수 후 최저가
        self.highPrice_date = buying_date # 최고가 날짜
        self.lowPrice_date = buying_date # 최저가 날짜

    def setCurPrice(self, cur_price, date): # 종목 현재가격 설정
        self.cur_price = cur_price
        self.estimated_amount = self.stock_cnt * cur_price
        if cur_price > self.high_price: 
            self.high_price = cur_price
            self.highPrice_date = date
        if cur_price < self.low_price: 
            self.low_price = cur_price
            self.lowPrice_date = date


ld_diffRation = lambda x, y: (x - y) / y * 100
three_com = lambda x: format(x, ',')

class VolumnSignal:

    def __init__(self, start_date, end_date, funding_money, volSigMul_S, volSigMul_E, maxHold_stockCnt, trgt_ror, max_lossRate,
                 maxHold_days, meanDays=20):
        '''
        :param start_date: 시뮬레이션 시작일
        :param end_date: 시뮬레이션 종료일
        :param funding_money: 투자금
        :param volSigMul_S: 종목 매수 신호(기준) 범위시작 값(평균 거래량 멀티플)
        :param volSigMul_E: 종목 매수 신호(기준) 범위종료 값(평균 거래량 멀티플)
        :param maxHold_stockCnt: 최대 보유 종목수
        :param trgt_ror: 목표 수익률
        :param maxHold_days: 최대 보유일수
        :param max_lossRate: 최대 손해율
        :param meanDays: 거래량 평균일 (기본값: 20일)
        '''

        self.start_date = start_date
        self.end_date = end_date
        self.cash = funding_money
        self.volSigMul_S = volSigMul_S
        self.volSigMul_E = volSigMul_E
        self.maxHold_stockCnt = maxHold_stockCnt
        self.trgt_ror = trgt_ror
        self.max_lossRate = max_lossRate
        self.maxHold_days = maxHold_days
        self.dic_holdStockInf = dict()  # 보유종목 정보 딕셔너리
        self.dic_buyTrgInf = dict()  # 구매 트리거 정보 딕셔너리
        self.meanDays = meanDays # 평균 산출일
        self.dic_tradingDf = dict() # 트레이딩 데이터 프레임
        self.dic_stockName = dict() # 종목이름
        self.dic_yearFR = dict() # 년단위 재무비율 데이터 (key: 년도)

        # 거래 데이터 로드
        self.__loadTradingData()

        # 재무비율 데이터 로드
        self.__loadFinacialRateData()


    # 거래 데이터 로드
    def __loadTradingData(self):
        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]
        cursCorp = STOCK_CROP_DATA_CLT.find({})
        for i, docStock in enumerate(cursCorp):  # 종목정보
            stock_code = docStock["stock_code"]
            stock_name = docStock["stock_name"]
            # 테스트 시작은 거래량 평균 산출일이 적용되어야 함으로 쿼리에서
            # 시작일에서 거래량 평균 산출일만큼 이전일로 구해야한다.
            start_date = str(self.start_date)
            dt_start = datetime.datetime.strptime(start_date, "%Y%m%d")
            dt_t_start = dt_start - datetime.timedelta(days=(self.meanDays+self.meanDays/5+1))
            start_date = dt_t_start.strftime("%Y%m%d")
            start_date = int(start_date)
            cursTradingData = stockDB.SP_DB["A" + stock_code].find({"$and": [{"날짜": {"$gte": start_date}},
                                                               {"날짜": {"$lte": self.end_date}}]},{"_id":0,"대비부호":0}).sort("날짜", pymongo.ASCENDING)
            # 쿼리 결과 cusor 객체를 => list => DataFrame으로 변경
            df = pd.DataFrame(list(cursTradingData))
            #날짜 컬럼을 인덱스로 사용
            df.set_index(df["날짜"], inplace=True)
            df.drop(columns=["날짜"], inplace=True) # 날짜 컬럼 삭제
            # 종목이 삼성전자 경우 날짜 인덱스를 테스트 기준날짜로 설정
            if stock_code == "005930":
                self.std_dateRng = df.index
                self.start_date = df.index[self.meanDays+int(self.meanDays/5)]
            # 종목 거래정보 데이터 프레임 저장
            self.dic_tradingDf[stock_code] = df
            #종목이름 사전화
            self.dic_stockName[stock_code] = stock_name
            print(i+1, ")", stock_name,"거래정보 로드")


    # 재무비율 데이터 로드
    def __loadFinacialRateData(self):
        # 재무비율 데이터 범위설정을 위한 년도 데이터
        startYear = int(self.start_date/10000)-1
        endYear = int(self.end_date/10000)-1
        # 년도 범위로 loop 수행
        for year in range(startYear, endYear+1):
            # 재무비율 컬렉션에서 해당 년도의 데이터 조회
            curYearFR = stockDB.FS_DB["YEAR_FR_DATA_CLT"].find({"year":str(year)})
            dfYearFR = pd.DataFrame() # 년도 재무비율 데이터프레임
            cnt =0
            for yearFR in curYearFR:
                stock_code = yearFR["stock_code"] # 종목코드
                fr_data = yearFR["FR"] # 재무비율 데이터
                srYearFR = pd.Series(fr_data, name=stock_code) # 종목 재무비율데이터(dict) => Series
                dfYearFR = pd.concat([dfYearFR, srYearFR], axis=1) # 종목 재무비율 데이터 데이터프레임에 추가
                stock_name = self.dic_stockName[stock_code]
                print(year,"년  ",cnt + 1, ")", stock_name, "재무비율정보 로드")
                cnt += 1
            self.dic_yearFR[year] = dfYearFR # 년도 재무비율 데이터 저장
            print(year, " 년도 재무비율데이터", cnt, "개 구축")

    # 종목 매수 금액
    def __buying_amount(self):
        spare_cnt = self.maxHold_stockCnt - len(self.lst_holdStockInf)
        return int(self.cash / spare_cnt)

    # 종목 매수
    def __buyingStock(self, stock_code, stock_name, dfTdAvgInf, stdTdData, date, slope):
        # 기준일 이전 평균 거래량 얻기
        volumnS = dfTdAvgInf["거래량"]
        priceS = dfTdAvgInf["종가"]
        volAvg = volumnS.mean()  # 평균거래량
        marketCapS = dfTdAvgInf["시가총액"]
        marketCapAvg = marketCapS.mean() # 평균산출일 기간 평균 시가총액        
        # 기준일 가격정보
        cur_volumn = stdTdData["거래량"]
        end_price = stdTdData["종가"]
        start_price = stdTdData["시가"]
        high_price = stdTdData["고가"]
        low_price = stdTdData["저가"]
        middle_price = int((high_price - low_price) / 2) + low_price  # 중간 주가
        # ====================== 종목 매수 ======================
        # 종목 매수조건
        # >> 1. 기 보유종목은 중복매수 하지 않음
        # >> 2. 최대 보유종목 수 미만일 때만 매수 시도
        # >> 3. 시총 1,000 미만 종목은 매수하지 않음

        # 1. 기 보유종목은 중복매수 하지 않음
        if marketCapAvg < 100000000000: return 0 # 시가총액이 1,000 미만 종목은 skip
        # 2. 현재 보유종목수가 최대 보유종목 수보다 적다면
        if len(self.dic_holdStockInf) >= self.maxHold_stockCnt: return 0
        # 3. 이미 보유종목이면 스킵
        if stock_code in self.dic_holdStockInf:
            holdStock = self.dic_holdStockInf[stock_code]
            holdStock.setCurPrice(end_price) # 종목의 현재가격 설정
            return 0
        # 가격 변화량이 0보다 크고 0.5보다 작은 경우만 매수 대상으로 선정
        if slope < 0 or slope > 0.5: return 0
        # 평균산출일 평균거래량 대비 기준일 거래량이 신호 거래량 비율 범위안에 있어야만 매수
        cur_volMul = cur_volumn / volAvg
        if cur_volMul < self.volSigMul_S or cur_volMul > self.volSigMul_E: return 0
        # 매수
        spareCnt = self.maxHold_stockCnt - len(self.dic_holdStockInf)  # 매수 가능 종목 수
        buyingAmount = self.cash / spareCnt  # 매수 가능 금액
        fee = middle_price * 0.00015  # 수수료
        buyStockCnt = int(buyingAmount / (middle_price + fee))
        if buyStockCnt == 0: return 0  # 종목을 구매할 수 있는 현금이 없음
        buyStock = HoldStockInfo(stock_code, stock_name, middle_price, buyStockCnt, date, slope, priceS.mean(), priceS.std())
        self.dic_holdStockInf[stock_code] = buyStock
        self.cash -= int((middle_price + fee)) * buyStockCnt
        print(f"========= 종목 매수 =========")
        print(f" 매수일: {date}")
        print(f" 매수종목: {stock_name}")
        print(f" 평균거래량: {three_com(volAvg)}")
        print(f" 거래량: {three_com(cur_volumn)}")
        print(f" 매수가격: {three_com(middle_price)}")
        print(f" 매수수량: {three_com(buyStockCnt)}")
        print(f" 매수금액(수수료포함): {three_com(int((middle_price + fee)) * buyStockCnt)}")                
        print(f"============================")
        return (buyStockCnt * end_price)
    
    # 종목 매도
    def __sellStock(self, idate, stock_code, middle_price, end_price):
        if stock_code not in self.dic_holdStockInf: return False  # 보유종목이 없다면 Skip
        hold_stock = self.dic_holdStockInf[stock_code]  # 보유종목 정보
        ror = ld_diffRation(middle_price, hold_stock.buying_price)  # 수익률
        hold_stock.setCurPrice(end_price)  # 종목의 현재가격 설정

        # 매도 전략1: 목표 수익률 달성 시 매도
        if ror > self.trgt_ror:
            sell_stock = True
            print("  > 목표수익률 달성")

        # 매도 전략2:
        # 초과 시 매도
        if sell_stock == False:
            std_date = str(idate)
            dt_std = datetime.datetime.strptime(std_date, "%Y%m%d")
            buy_date = str(hold_stock.buying_date)
            dt_buy = datetime.datetime.strptime(buy_date, "%Y%m%d")
            day_delta = dt_std - dt_buy
            if day_delta.days >= self.maxHold_days:
                sell_stock = True
                print("  > 보유일 초과")
            # 매도 전략3: 손해률 초과 시 매도
            if ror < 0:
                if abs(ror) > self.max_lossRate:
                    sell_stock = True
                    print("  > 손해 초과")
    
            # 주식 매도 결정
            if sell_stock == True:
                sold_amount = hold_stock.stock_cnt * middle_price
                fee = int(sold_amount * 0.00015)  # 수수료
                tax = int(sold_amount * 0.003)  # 세금
                sold_amount -= (fee + tax)
                self.cash += sold_amount
    
                print(f"========= 종목 매도 ========= ")
                print(f" 매도일: {idate}")
                print(f" 매도종목: {self.dic_stockName[stock_code]}")
                print(f" 매도가격: {three_com(middle_price)}")
                print(f" 매도수량: {three_com(hold_stock.stock_cnt)}")
                print(f" 매도금액(수수료포함): {three_com(sold_amount)}")                
                print(" 수익률: {:.1f}%".format(ror))
                print(f" 보유일: {three_com(day_delta.days)}")
                print(f" 매수시 가격 : { three_com(hold_stock.buying_price)}")
                print(" 매수시 가격 Slope: {:.1f}%".format(hold_stock.price_ll_slope))
                print(f" 최고가(날짜): {three_com(hold_stock.high_price)}({hold_stock.highPrice_date})")
                print(f" 최저가(날짜): {three_com(hold_stock.low_price)}({hold_stock.lowPrice_date})")
                print(f"=============================")
                del self.dic_holdStockInf[stock_code]  # 보유종목 삭제
                return True
            
        hold_stock.stock_cnt(end_price) # 종가 정보 입력
        return False    

    def sectionPriceSlope(self, priceS):
        sCnt = int(priceS.index.shape[0] / 4) #한 구역의 데이터 갯수
        if sCnt == 0: return None
        ltSlope=[]
        for i in range(4):
            sIdx = i*sCnt
            if i < 3:
                list = priceS[sIdx: sIdx+sCnt].tolist()
                slope, intercept, r_value, p_value, stderr = stats.linregress(range(len(list)),
                                                                              list)
                ltSlope.append(slope)
            else:
                list = priceS[sIdx:].tolist()
                slope, intercept, r_value, p_value, stderr = stats.linregress(range(len(list)),
                                                                              list)
                ltSlope.append(slope)
        return ltSlope

    def run(self):
        for idate in self.std_dateRng: # 기준일 범위에서 loop
            if idate < self.start_date: continue # 기준일이 테스트 이전일이면 스킵
            stock_asset = 0  # 주식 자산
            for stock_code in self.dic_tradingDf:  # 종목정보
                stock_name = self.dic_stockName[stock_code]
                dfTdInf = self.dic_tradingDf[stock_code] # 거래정보 데이터 프레임
                if idate not in dfTdInf.index: continue # 종목 거래정보 데이터에 기준일 데이터가 없다면 스킵
                stdTdData = dfTdInf.loc[idate] # 기준일 거래량 데이터
                dfTdAvgInf = dfTdInf[dfTdInf.index < idate] # 기준일 으로 이후 데이터는 필터링
                if dfTdAvgInf.shape[0] == 0:continue # 기준일 이전의 데이터가 없다면 스킵
                dataCnt = dfTdAvgInf.index.shape[0] # 데이터 프레임의 index의 수(데이터 프레임의 데이터 갯수)
                if self.meanDays < dataCnt: # 데이터 갯수가 평균 산출일 보다 같거나 많다면
                    sDate = dfTdAvgInf.index[dataCnt - self.meanDays] # 평균산출일이 시작되는 날짜를 구해
                    dfTdAvgInf = dfTdAvgInf[dfTdAvgInf.index >= sDate] # 날짜 이전은 데이터는 필터링
                priceS = dfTdAvgInf["종가"]
                # 기준일 가격정보
                cur_volumn = stdTdData["거래량"]
                end_price = stdTdData["종가"]
                start_price = stdTdData["시가"]
                high_price = stdTdData["고가"]
                low_price = stdTdData["저가"]
                middle_price = int((high_price - low_price) / 2) + low_price  # 중간 주가
                # 평균산출일 기간동안의 가격 선형회귀를 구해 가격의 추이(상승/하락)를 파악
                '''
                slope: 회귀선의 기울기입니다.
                intercept: 회귀선의 절편입니다.
                rvalue: 상관 계수.
                pvalue: 귀무 가설이 있는 가설 검정의 양측 p-값 기울기가 0인지 여부, t-분포와 함께 Wald Test를 사용합니다. 검정 통계량
                stderr: 추정된 그라데이션의 표준 오차입니다.        
                '''
                slope, intercept, r_value, p_value, stderr = stats.linregress(range(priceS.index.shape[0]),
                                                                              priceS.tolist())                    
                # 종목매수
                buyAmount = self.__buyingStock(stock_code, stock_name, dfTdAvgInf, stdTdData, idate, slope)
                if buyAmount > 0: continue # 종목을 매수 했다면(구매 금액이 있다면) 기준일에 해당 종목에 대한 다른 액션은 없음

                # 종목 매도
                self.__sellStock(idate, stock_code, middle_price, end_price)

            print(f"> 날짜: {idate}, 현금잔액: {three_com(self.cash)}")
            print(f"> 보유종목수: {len(self.dic_holdStockInf)}, 주식자산: {three_com(stock_asset)}")
            
            # 보유종목 추정자산
            stock_asset = 0
            for stock_code in self.dic_holdStockInf:
                hold_stock = self.dic_holdStockInf[stock_code]
                stock_asset += hold_stock.estimated_amount
            
            print(f"> 평가자산: {three_com(self.cash + stock_asset)}")
            print("----------------------------------------------")

if __name__ == "__main__":
    a = 0
    print(__file__)
    print(os.path.abspath(__file__))
    volumnSignal = VolumnSignal(20170301, 20210524, 3000000, 3, 10, 10, 70, 30, 120, 20);
    volumnSignal.run();