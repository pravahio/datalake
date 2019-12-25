import json

from datalake import Datalake
from mesh_gtfsr.mesh import MeshGTFSR
from google.protobuf.json_format import MessageToJson

geospace = [
    '/in/haryana/gurugram',
    '/in/delhi',
    '/in/mp/indore',
    '/in/karnataka/mysore',
    '/us/oregon/portland',
    '/us/kentucky/louisville',
    '/us/massachusetts/bostonMetro',
    '/us/california/contraCostaCounty',
    '/us/connecticut',
    '/us/mta',
    '/aus/nsw',
    '/aus/victoria/kingston'
]

def main():
    c = MeshGTFSR('rpc.pravah.io:5555')
    feed = c.subscribe(geospace)

    datalake = Datalake('pravah', '', c.get_channel())

    for m, c in feed:
        jsonObj = json.loads(MessageToJson(m))
        print(c)
        obj = datalake.insert(c, jsonObj)
        print(str(obj) + ': ' + c)

if '__main__' == __name__:
    main()