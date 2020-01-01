import json
import pytz

from dlake import Datalake

geospace = [
    '/in/ncr'
]

def main():

    # Available channels and geospaces: https://github.com/pravahio/go-mesh/wiki/Geospaces
    # '/PublicBus', '/AirQuality', '/SolarPowerProduction'
    # Geospaces would differ from channel to channel.
    datalake = Datalake('pravah', '', '/AirQuality')

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
    cur = datalake.get({
        "item.stations.id": "site_5050"
        }, 
        past_minutes=20 # mins
    )

    print('Total Results: ' + str(cur.count()))
    ist = pytz.timezone('Asia/Kolkata')
    for c in cur:
        print(c['_id'].generation_time.astimezone(ist))

if '__main__' == __name__:
    main()