from pymongo import MongoClient

class StockDB:

    def __init__(self):
        mongoClient = MongoClient('mongodb://{0}:{1}@127.0.0.1'.format("crexy", "lowstar9130!"))
        self.FS_DB = mongoClient.Stock_Investment  # 재무정보 DB
        self.SP_DB = mongoClient.Stock_Price  # 종목시세정보 DB


stockDB = StockDB()