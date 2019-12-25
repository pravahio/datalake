from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId

class Datalake:
    def __init__(self, usr, pwd, channel):

        self.client = MongoClient('mongodb://13.232.47.177:27017/datalake', username=usr, password=pwd)
        
        channel = channel.replace('/', '').lower()
        print('Working on collection: ' + channel)
        self.collection = self.client.datalake[channel]

    def insert(self, geospace, obj):
        data = {
            'geospace': geospace,
            'item': obj
        }
        return self.collection.insert(data)
    
    # past: 60 mins
    # start: 2019/12/23 00:00:00
    def get(self, obj, start='', end='', past=60):
        if isinstance(obj, ObjectId):
            return self.collection.find_one({
                '_id': obj
            })
        elif isinstance(obj, dict):
            if 'geospace' in obj:
                if start != '' and end != '':
                    start_date = ObjectId.from_datetime(datetime.strptime(start, '%Y/%m/%d %H:%M:%S'))
                    end_date = ObjectId.from_datetime(datetime.strptime(end, '%Y/%m/%d %H:%M:%S'))
                    return self.collection.find({
                        '_id': {
                            '$gte': start_date,
                            '$lte': end_date
                        },
                        'geospace': obj['geospace'],
                    })
                elif start != '':
                    print('this')
                    start_date = ObjectId.from_datetime(datetime.strptime(start, '%Y/%m/%d %H:%M:%S'))
                    return self.collection.find({
                        '_id': {
                            '$gte': start_date
                        },
                        'geospace': obj['geospace'],
                    })
                elif end != '':
                    end_date = ObjectId.from_datetime(datetime.strptime(end, '%Y/%m/%d %H:%M:%S'))
                    return self.collection.find({
                        '_id': {
                            '$lte': end_date
                        },
                        'geospace': obj['geospace'],
                    })
                else:
                    date = datetime.utcnow() - timedelta(minutes=past) 
                    start_date = ObjectId.from_datetime(date)
                    return self.collection.find({
                        '_id': {
                            '$gte': start_date
                        },
                        'geospace': obj['geospace'],
                    })
        raise Exception

