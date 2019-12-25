from pymongo import MongoClient

class Datalake:
    def __init__(self, usr, pwd, channel):

        self.client = MongoClient('mongodb://13.232.47.177:27017/datalake', username=usr, password=pwd)
        
        channel = channel.replace('/', '').lower()
        print('Working on collection: ' + channel)
        self.db = self.client.datalake[channel]

    def insert(self, geospace, obj):
        data = {
            'geospace': geospace,
            'item': obj
        }
        return self.db.insert(data)
    
    def get(self, obj):
        return self.db.find_one({
            '_id': obj
        })
