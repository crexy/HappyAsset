from pymongo import MongoClient
from ASECipher import AESCipher


class StockDB:

    def __init__(self):

        # 시크릿키 읽기
        f = open("security_key", 'r')
        security_key = f.readline()
        f.close()
        aesCipher = AESCipher(security_key)
        # 데이터 베이스 계정 읽기

        f = open("databaseInfo.dat", 'r')
        id_encrypt = f.readline()
        pwd_encrypt = f.readline()

        id = aesCipher.decrypt_str(id_encrypt)
        pwd = aesCipher.decrypt_str(pwd_encrypt)

        mongoClient = MongoClient('mongodb://{0}:{1}@127.0.0.1'.format(id, pwd))
        self.FS_DB = mongoClient.Stock_Investment  # 재무정보 DB
        self.SP_DB = mongoClient.Stock_Price  # 종목시세정보 DB


stockDB = StockDB()