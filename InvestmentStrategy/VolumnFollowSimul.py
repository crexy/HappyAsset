
import pandas as pd
import pymongo
from StockDB import stockDB
import datetime


# 보유종목 정보
class HoldStockInfo:
    def __init__(self, stock_code, stock_name, buying_price, stock_cnt, buying_date):
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


# 매수 트리거 정보
class BuyingTrigerInfo:
    def __init__(self, stock_code, stock_name, mean_volumn, trg_volumn, trg_price, trg_date):
        '''
        :param stock_code: 종목코드
        :param stock_name: 종목명
        :param mean_volumn: 평균거래량
        :param trg_volumn: 종목 매수 트리거(기준) 거래량
        :param trg_price: 트리거 가격
        :param trg_date: 트리거 종목
        '''
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.mean_volumn = mean_volumn
        self.trg_volumn = trg_volumn
        self.trg_price = trg_price
        self.trg_date = trg_date

ld_diffRation = lambda x, y: (x - y) / y * 100
three_com = lambda x : format(x, ',')

class VolumnFollowStrategy:

    def __init__(self, start_date, end_date, funding_money, trg_vol_multiple, maxHold_stockCnt, trgt_ror, max_lossRate, maxHold_days, meanDays = 20):
        '''
        :param start_date: 시뮬레이션 시작일
        :param end_date: 시뮬레이션 종료일
        :param funding_money: 투자금
        :param trg_vol_multiple: 종목 매수 트리거(기준) 평균 거래량 멀티플
        :param maxHold_stockCnt: 최대 보유 종목수
        :param trgt_ror: 목표 수익률
        :param maxHold_days: 최대 보유일수
        :param max_lossRate: 최대 손해율
        :param meanDays: 거래량 평균일 (기본값: 20일)
        '''

        self.start_date = start_date
        self.end_date = end_date
        self.cash = funding_money
        self.trg_vol_multiple = trg_vol_multiple
        self.maxHold_stockCnt = maxHold_stockCnt
        self.trgt_ror = trgt_ror
        self.max_lossRate = max_lossRate
        self.maxHold_days = maxHold_days
        self.dic_holdStockInf = dict()  # 보유종목 정보 딕셔너리
        self.dic_buyTrgInf = dict() # 구매 트리거 정보 딕셔너리
        self.meanDays = meanDays

    # 종목 매수 금액
    def __buying_amount(self):
        spare_cnt = self.maxHold_stockCnt - len(self.lst_holdStockInf)
        return int(self.cash / spare_cnt)


    # 종목 매수
    def __buyingStock(self, stock_code, stock_name, ltPriceInf, date, ltPrevDate):

        end_price = ltPriceInf["종가"]
        start_price = ltPriceInf["시가"]
        high_price = ltPriceInf["고가"]
        low_price = ltPriceInf["저가"]
        middle_price = int((high_price - low_price) / 2) + low_price  # 중간 주가

        # 현재 보유종목수가 최대 보유종목 수보다 적다면
        # 종목 추가 매수 가능
        if len(self.dic_holdStockInf) >= self.maxHold_stockCnt: return 0

        if stock_code in self.dic_holdStockInf: return 0  # 이미 보유종목이면 스킵

        # 트리거 종목 확인 후
        # 매수 조건에 부합하는 종목 매수
        if stock_code not in self.dic_buyTrgInf: return 0  # 매수 대상 종목이 없는 경우

        buyingtrgInf = self.dic_buyTrgInf[stock_code]

        highEnd_diffR = ld_diffRation(high_price, end_price)  # 고가-종가 차이 비율

        # 구매대상 종목 등록 후 1일 경과
        oneDayPrev = 0
        if len(ltPrevDate) == 1:
            oneDayPrev = ltPrevDate[0]
        elif len(ltPrevDate) == 2:
            oneDayPrev = ltPrevDate[1]

        if buyingtrgInf.trg_date == oneDayPrev:
            if end_price < start_price or highEnd_diffR > 5:  # 가격에 하락하거나 고가 대비 종가가 5% 이상 차이가 날 시
                del self.dic_buyTrgInf[stock_code] # 해당 종목 구매대상에서 삭제
                return 0

        if len(ltPrevDate) < 2: return 0 # 시뮬레이션 시작 후 2일이 경과 되지 않은 경우 리턴
        
        if buyingtrgInf.trg_date != ltPrevDate[0]: return 0 # 구매대상 종목의 구매결정일(대상 등록 후 2일)이 아니라면 리턴

        if buyingtrgInf.trg_price > middle_price: return 0 # 주가가 내려갔다면 리턴
        # 매수
        spareCnt = self.maxHold_stockCnt - len(self.dic_holdStockInf)  # 매수 가능 종목 수
        buyingAmount = self.cash / spareCnt  # 매수 가능 금액
        fee = middle_price * 0.00015  # 수수료
        buyStockCnt = int(buyingAmount / (middle_price + fee))
        if buyStockCnt == 0: return 0  # 종목을 구매할 수 있는 현금이 없음
        buyStock = HoldStockInfo(stock_code, stock_name, middle_price, buyStockCnt, date)
        self.dic_holdStockInf[stock_code] = buyStock
        self.cash -= int((middle_price + fee)) * buyStockCnt

        print(f"========= 종목 매수 =========")
        print(f" 매수일: {date}")
        print(f" 매수종목: {stock_name}")
        print(f" 평균거래량: {three_com(buyingtrgInf.mean_volumn)}")
        print(f" 거래량: {three_com(buyingtrgInf.trg_volumn)}")
        print(f" 매수가격: {three_com(middle_price)}")
        print(f" 매수수량: {three_com(buyStockCnt)}")
        print(f" 매수금액(수수료포함): {three_com(int((middle_price + fee)) * buyStockCnt)}")
        # print(f" 현금잔액: {three_com(self.cash)}")
        print(f"============================")

        del self.dic_buyTrgInf[stock_code]  # 구매 대상 종목 삭제

        return (buyStockCnt * end_price)

    def run(self):
        start_date = self.start_date  # 시뮬레이션 시작일
        end_date = self.end_date      # 시뮬레이션 종료일        
        trg_vol_multiple = self.trg_vol_multiple    # 거래량 트리거 멀티플(기준)

        STOCK_CROP_DATA_CLT = stockDB.FS_DB["STOCK_CROP_DATA_CLT"]

        sdateInfo = stockDB.SP_DB["A005930"].find({"날짜":{"$lt": start_date}},
                                      {"_id":0,"날짜":1}).sort("날짜", pymongo.DESCENDING).limit(1)

        # sdateInfo = stockDB.SP_DB["A005930"].find_one({"날짜": {"$lt": start_date}},
        #                                               {"_id": 0, "날짜": 1})
        start_date = sdateInfo[0]["날짜"]
        dateCurs = stockDB.SP_DB["A005930"].find({ "$and":[{"날짜":{"$gte":start_date}},
                                                           {"날짜":{"$lte":end_date}}]},
                                                 {"_id":0,"날짜":1}).sort("날짜", pymongo.ASCENDING)

        ltPrevDate = list() #날짜 리스트
        #prevDate = -1 # 거래일 기준 1일 이전일
        for docDate in dateCurs:
            date = docDate["날짜"]

            stock_asset = 0 # 주식 자산

            cursCorp = STOCK_CROP_DATA_CLT.find({})
            for docStock in cursCorp:  # 종목정보
                stock_code = docStock["stock_code"]
                stock_name = docStock["stock_name"]
                #if stock_name != '한일현대시멘트': continue
                cursPrice = stockDB.SP_DB["A" + stock_code].find({"날짜": {"$lte": date}}).sort("날짜",
                                                                                             pymongo.DESCENDING).limit(
                    self.meanDays+1)

                # 일일 거래정보 데이터 => 리스트화
                ltPriceInf = list(cursPrice)

                if len(ltPriceInf) < self.meanDays+1: continue

                cur_volumn = ltPriceInf[0]["거래량"]
                end_price = ltPriceInf[0]["종가"]
                start_price = ltPriceInf[0]["시가"]
                high_price = ltPriceInf[0]["고가"]
                low_price = ltPriceInf[0]["저가"]
                middle_price = int((high_price - low_price) / 2) + low_price  # 중간 주가


                # ====================== 매도 로직 ======================
                sell_stock = False
                # 현재 보유 주식 매도 결정
                if stock_code in self.dic_holdStockInf:
                    hold_stock = self.dic_holdStockInf[stock_code]

                    ror = ld_diffRation(middle_price, hold_stock.buying_price) # ror: 수익률

                    # 매도 전략1: 목표 수익률 달성 시 매도
                    if ror > self.trgt_ror:
                        sell_stock = True
                        print("  > 목표수익률 달성")

                    # 매도 전략2:
                    # 초과 시 매도
                    if sell_stock == False:
                        std_date = str(date)
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

                    # 매도 전략4: 매수 후 다음날 주가 하락 시 매도
                    # if ltPriceInf[1]["날짜"] == hold_stock.buying_date and\
                    #     middle_price < hold_stock.buying_price:
                    #     sell_stock = True
                    #     print("  > 매수 다음날 주가 하락")

                    # 주식 매도 결정
                    if sell_stock == True:
                        sold_amount = hold_stock.stock_cnt * middle_price
                        fee = int(sold_amount*0.00015) #수수료
                        tax = int(sold_amount*0.003) #세금
                        sold_amount -=(fee+tax)
                        self.cash += sold_amount

                        print(f"========= 종목 매도 ========= ")
                        print(f" 매도일: {date}")
                        print(f" 매도종목: {stock_name}")
                        print(f" 매도가격: {three_com(middle_price)}")
                        print(f" 매도수량: {three_com(hold_stock.stock_cnt)}")
                        print(f" 매도금액(수수료포함): {three_com(sold_amount)}")
                        #print(f" 현금잔액: {three_com(self.cash)}")
                        print(" 수익률: {:.1f}%".format(ror))
                        print(f" 보유일: {three_com(day_delta.days)}")
                        print(f"=============================")

                        del self.dic_holdStockInf[stock_code]  # 보유종목 삭제
                    else:
                        stock_asset += (hold_stock.stock_cnt * end_price);

                # ====================== 매도 로직 ======================

                # 마지막 가격 정보 저장
                lastPriceInfo = ltPriceInf[0]

                del ltPriceInf[0]

                # 일일 거래정보 데이터 프레임
                dfPriceInf = pd.DataFrame(ltPriceInf)
                volumnMean = dfPriceInf["거래량"].mean()  # 20일 평균 거래량

                if volumnMean < 1000: continue # 평균 거래량이 1000주 미만이면 스킵

                # 거래량이 평균거래량의 트리거 멀티플 기준을 넘어서면
                vol_multiple = lastPriceInfo["거래량"]/volumnMean
                if vol_multiple > self.trg_vol_multiple:
                    #and lastPriceInfo["시가총액"] > 100000000000: #시가총액이 천억이상 종목만 매수대상 등록
                
                    if stock_code not in self.dic_holdStockInf: # 보유 종목에 없는 종목만 매수 대상 종목으로 등록
                        # 매수 대상 종목으로 등록
                        
                        highEnd_diffR = ld_diffRation(high_price, end_price)  # 고가-종가 차이 비율
                        
                        # 거래량 트리거 발생일에 가격 상승 및 고가 종가 차이가 7%이하시에만 매수 대상종목 등록
                        if end_price > start_price and highEnd_diffR < 7:                        
                            buyingtrgInf = BuyingTrigerInfo(stock_code, stock_name, volumnMean, lastPriceInfo["거래량"], end_price, date)
                            #print(f"   - {stock_name}) 거래량 멀티플: {vol_multiple}")
                            self.dic_buyTrgInf[stock_code] = buyingtrgInf
                else: # 현재 종목이 거래량 트리거 상태가 아닌 경우
                    # 종목 구매
                    stock_asset += self.__buyingStock(stock_code, stock_name, lastPriceInfo, date, ltPrevDate)

                    if stock_code in self.dic_buyTrgInf and len(ltPrevDate) == 2: # 매수 대상 종목이 있는 경우
                        buyingTrg = self.dic_buyTrgInf[stock_code]
                        if buyingTrg.trg_date == ltPrevDate[0]: #매수 대상 등록 후 2일 경과 시
                            del self.dic_buyTrgInf[stock_code] # 구매 대상 종목 삭제

            # 이전 거래일 리스트 갱신, 이전 거래일 2일 만 저장
            if len(ltPrevDate) == 2:
                del ltPrevDate[0]
            ltPrevDate.append(date)

            print(f"> 날짜: {date}, 현금잔액: {three_com(self.cash)}")
            print(f"> 보유종목수: {len(self.dic_holdStockInf)}, 주식자산: {three_com(stock_asset)}")
            print(f"> 평가자산: {three_com(self.cash+stock_asset)}")
            print("----------------------------------------------")

if __name__ == "__main__":
    a = 0

    volumnFollow = VolumnFollowStrategy(20170301, 20210524, 3000000, 30, 10, 50, 10, 70, 20);
    volumnFollow.run();