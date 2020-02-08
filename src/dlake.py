from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId

class Datalake:
    def __init__(self, usr, pwd, channel):
        self.client = MongoClient('mongodb://datalake.pravah.io:27017/datalake', username=usr, password=pwd)
        
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
    def get(self, query={}, **kwargs):
        if isinstance(query, ObjectId):
            return self.collection.find_one({
                '_id': query
            })
        elif isinstance(query, dict):
            kwargs = self.set_time_args_defaults(**kwargs)
            self.get_query_for_time_bound(query, kwargs[TimeParam.Start], kwargs[TimeParam.End], kwargs[TimeParam.PastDays], kwargs[TimeParam.PastHours], kwargs[TimeParam.PastMinutes], kwargs[TimeParam.PastSeconds])
            print(query)
            return self.collection.find(query)

    def aggregate(self, group_by='', push={}, pipeline=[], match={}, **kwargs):
        
        final_agg = []

        kwargs = self.set_time_args_defaults(**kwargs)

        self.get_query_for_time_bound(match, kwargs[TimeParam.Start], kwargs[TimeParam.End], kwargs[TimeParam.PastDays], kwargs[TimeParam.PastHours], kwargs[TimeParam.PastMinutes], kwargs[TimeParam.PastSeconds])
        if bool(match):
            final_agg.append({
                "$match": match
            })
        final_agg = final_agg + pipeline

        return self.collection.aggregate(final_agg)

    def get_query_for_time_bound(self, query, start, end, past_days, past_hours, past_minutes, past_seconds):
        if start != '' and end != '':
            start_date = ObjectId.from_datetime(datetime.strptime(start, '%Y/%m/%d %H:%M:%S').astimezone())
            end_date = ObjectId.from_datetime(datetime.strptime(end, '%Y/%m/%d %H:%M:%S').astimezone())
            query['_id'] = {
                '$gte': start_date,
                '$lte': end_date
            }
        elif start != '':
            print('this')
            start_date = ObjectId.from_datetime(datetime.strptime(start, '%Y/%m/%d %H:%M:%S').astimezone())
            query['_id'] = {
                '$gte': start_date
            }
        elif end != '':
            end_date = ObjectId.from_datetime(datetime.strptime(end, '%Y/%m/%d %H:%M:%S').astimezone())
            query['_id'] = {
                '$lte': end_date
            }
        else:
            date = datetime.utcnow() - timedelta(hours=past_hours+24*past_days, minutes=past_minutes, seconds=past_seconds) 
            start_date = ObjectId.from_datetime(date)
            query['_id'] = {
                '$gte': start_date
            }
        return query
    
    def set_time_args_defaults(self, **kwargs):
        kwargs.setdefault(TimeParam.Start, '')
        kwargs.setdefault(TimeParam.End, '')
        kwargs.setdefault(TimeParam.PastDays, 0)
        kwargs.setdefault(TimeParam.PastHours, 0)
        kwargs.setdefault(TimeParam.PastMinutes, 1)
        kwargs.setdefault(TimeParam.PastSeconds, 0)

        return kwargs

class TimeParam:
    Start = 'start'
    End = 'end'
    PastDays = 'past_days'
    PastHours = 'past_hours'
    PastMinutes = 'past_minutes'
    PastSeconds = 'past_seconds'
    