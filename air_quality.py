import json

from datalake import Datalake
from mesh_air_quality.main import MeshAirQuality
from google.protobuf.json_format import MessageToJson

geospace = [
    '/in/ncr'
]

def main():
    c = MeshAirQuality('rpc.pravah.io:5555')
    feed = c.subscribe(geospace)

    datalake = Datalake('pravah', '', c.get_channel())

    for m, c in feed:
        jsonObj = json.loads(MessageToJson(m))

        obj = datalake.insert(c, jsonObj)
        print(datalake.get(obj))

if '__main__' == __name__:
    main()