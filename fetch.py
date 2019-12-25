import json

from datalake import Datalake

geospace = [
    '/in/ncr'
]

def main():

    # Available channels and geospaces: https://github.com/pravahio/go-mesh/wiki/Geospaces
    # '/PublicBus', '/AirQuality', '/SolarPowerProduction'
    datalake = Datalake('pravah', 'Pr@v@h@dm!n', '/AirQuality')

    # All the data after 'start' datetime
    #cur = datalake.get({'geospace': '/in/ncr'}, start='2019/12/24 18:00:00')

    # All the data before 'end' datetime
    #cur = datalake.get({'geospace': '/in/ncr'}, start='2019/12/24 18:00:00')

    # data between 'start' and 'end' datetime
    #cur = datalake.get({'geospace': '/in/ncr'}, start='2019/12/24 18:00:00', end='2019/12/25 18:00:00')

    # data from the past 60 mins
    cur = datalake.get({'geospace': '/in/ncr'}, past=120)

    print('Total Results: ' + str(cur.count()))
    for c in cur:
        print(c['_id'].generation_time)

if '__main__' == __name__:
    main()