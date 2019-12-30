import json
import pytz

from datalake import Datalake

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

    # data from the past 120 mins
    #cur = datalake.get({'geospace': '/in/ncr'}, past=120)

    # query your data
    cur = datalake.get({
        "item.stations.id": "site_5050"
        }, 
        past=20 # mins
    )

    print('Total Results: ' + str(cur.count()))
    ist = pytz.timezone('Asia/Kolkata')
    for c in cur:
        print(c['_id'].generation_time.astimezone(ist))

if '__main__' == __name__:
    main()