from pymongo import MongoClient

class StockInvestDB:

    def __init__(self):
        mongoClient = MongoClient('mongodb://{0}:{1}@192.168.219.107'.format("crexy", "lowstar9130!"))
        self.database = mongoClient.Stock_Investment


stockDB = StockInvestDB()