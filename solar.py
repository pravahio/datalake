import json

from datalake import Datalake
from mesh_solar_power_production.main import MeshSolarPowerProduction
from google.protobuf.json_format import MessageToJson

geospace = [
    '/in/delhi'
]

def main():
    c = MeshSolarPowerProduction('rpc.pravah.io:5555')
    feed = c.subscribe(geospace)

    datalake = Datalake('pravah', '', c.get_channel())

    for m, chan in feed:
        jsonObj = json.loads(MessageToJson(m))

        obj = datalake.insert(chan, jsonObj)
        print('Saving [' + c.get_channel() + ' -> ' + str(geospace) + '] at ' + str(obj))

if '__main__' == __name__:
    main()