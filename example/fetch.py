import json
import pytz

from dlake import Datalake

def main():

    # Available channels and geospaces: https://github.com/pravahio/go-mesh/wiki/Geospaces
    # '/PublicBus', '/AirQuality', '/SolarPowerProduction'
    # Geospaces would differ from channel to channel.
    datalake = Datalake(
        channel = '/AirQuality',
        auth_token = 'X5I2l8eyRCivN2hwBbGXXQ42BCF5AQSziRJQz_rRoGlg'
    )

    # All the data after 'start' datetime
    #cur = datalake.get({'geospace': '/in/ncr'}, start='2019/12/24 18:00:00')

    # All the data before 'end' datetime
    #cur = datalake.get({'geospace': '/in/ncr'}, start='2019/12/24 18:00:00')

    # data between 'start' and 'end' datetime
    #cur = datalake.get({'geospace': '/in/ncr'}, start='2019/12/24 18:00:00', end='2019/12/24 21:00:00')

    # data from the past 1 hour
    #cur = datalake.get({'geospace': '/in/ncr'}, past_hours=1) 

    # data from the past 120 mins
    #cur = datalake.get({'geospace': '/in/ncr'}, past_minutes=120)

    # data from the past 30 second
    #cur = datalake.get({'geospace': '/in/ncr'}, past_seconds=30)

    # or combination of hours, minutes and seconds
    #cur = datalake.get({'geospace': '/in/ncr'}, past_hours=1, past_minutes=13, past_seconds=10)

    # You can also query the data using mongoDB syntax
    cur = datalake.get(
        query = {
            'item.stations.id': "site_104"
        },
        past_hours=3
    )
    """ cur = datalake.aggregate(pipeline=[
        {
            "$group": {
                "_id": "$item.stations.id",
                "arr": {
                    "$push": "$item.stations.id"
                }
            }
        }
    ],
    match={"item.stations.id": "site_104"},
    past_minutes=1000) """
    
    if 'error' not in cur:
        print('Total Results: ' + str(len(cur)))
        for c in cur:
            print(c['item']['stations'][0]['dataList'])
    else:
        print(cur)

if '__main__' == __name__:
    main()