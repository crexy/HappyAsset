from pymongo import MongoClient
from pymongo import UpdateOne
from operator import itemgetter
import csv
#from scipy import stats
from StockDB import stockDB

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

        for x in list_totalAsset:
            if PSHS not in x[BS]:
                print(x['stock_code'])

        # 종목코드:지배주주지분 자산 map 생성
        dicTotalAsset = {x['stock_code']: x[BS][PSHS] for x in list_totalAsset}

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

            issued_cnt = doc['issued_shares_num']
            treasury_cnt = doc['treasury_stock']

            if treasury_cnt == 'N/A': continue
            if 'cns_year' not in doc: continue

            # 컨센서스 데이터가 존재하는 종목인 경우 년도 컨센서스의 ROE를 사용하고
            if SALES in doc['cns_year']:
                dic_cns_year = doc['cns_year']
                if 'ROE' in dic_cns_year:
                    if dic_cns_year['ROE'] != 'N/A':
                        roe = dic_cns_year['ROE']
            else:  # 컨센서스 데이터가 존재하지 않는 종목은 분기 추정 ROE를 사용함
                dic_cns_quarter = doc['cns_quarter']
                if 'ROE' in dic_cns_quarter:
                    if dic_cns_quarter['ROE'] != 'N/A':
                        roe = dic_cns_quarter['ROE']

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

            if (bve != 'N/A' and roe != 'N/A'):
                stock_name = doc['stock_name']
                # print(f'{rim_cnt}){stock_name}')
                bve *= 100000000
                val100 = self.__calc_S_RIM(bve, roe, discntR, 1, shareCnt)  # 적정가격
                val090 = self.__calc_S_RIM(bve, roe, discntR, 0.9, shareCnt)  # 10% 할인가격
                val080 = self.__calc_S_RIM(bve, roe, discntR, 0.8, shareCnt)  # 20% 할인가격

                list_rim = [doc['stock_code'], val100, val090, val080]
                list_bulk.append(list_rim)

                # print(f'{rim_cnt}){stock_name}: {list_rim}')
                rim_cnt += 1

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

#================================ DataAnalysis =====================================

dataAnalysis = DataAnalysis()

if __name__ == "__main__":
    dataAnalysis.updateAll_S_RIM(2020, 4) #S-RIM 가격 정보 Update