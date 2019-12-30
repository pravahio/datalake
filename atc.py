import json

from datalake import Datalake
from mesh_atc.main import MeshATC
from google.protobuf.json_format import MessageToJson

geospace = [
    '/in/agartala',
    '/in/agra',
    '/in/ahmedabad',
    '/in/aizwal',
    '/in/akola',
    '/in/allahabad',
    '/in/amritsar',
    '/in/aurangabad',
    '/in/bagdogra',
    '/in/balurghat',
    '/in/belgaum',
    '/in/bengaluru',
    '/in/bhavnagar',
    '/in/bhopal',
    '/in/bhubaneswar',
    '/in/bilaspur',
    '/in/calicut',
    '/in/chandigarh',
    '/in/chennai',
    '/in/coimbatore',
    '/in/coochbehar',
    '/in/dehradun',
    '/in/delhi',
    '/in/dibrugarh',
    '/in/dimapur',
    '/in/gaya',
    '/in/goa',
    '/in/guwahati',
    '/in/gwalior',
    '/in/hubli',
    '/in/hyderabad',
    '/in/imphal',
    '/in/indore',
    '/in/jabalpur',
    '/in/jaipur',
    '/in/jammu',
    '/in/jodhpur',
    '/in/jorhat',
    '/in/kadapa',
    '/in/kailashahar',
    '/in/kamalpur',
    '/in/kandla',
    '/in/kanpur',
    '/in/keshod',
    '/in/khajuraho',
    '/in/khowai',
    '/in/kochi',
    '/in/kolhapur',
    '/in/kolkata',
    '/in/kota',
    '/in/kullu',
    '/in/leh',
    '/in/lilabari',
    '/in/lucknow',
    '/in/ludhiana',
    '/in/madurai',
    '/in/malda',
    '/in/mangalore',
    '/in/mumbai',
    '/in/muzzafarpur',
    '/in/mysore',
    '/in/nagpur',
    '/in/pantnagar',
    '/in/patna',
    '/in/pondicherry',
    '/in/porbandar',
    '/in/portblair',
    '/in/pune',
    '/in/raipur',
    '/in/rajahmundry',
    '/in/rajkot',
    '/in/ranchi',
    '/in/rupsi',
    '/in/salem',
    '/in/satna',
    '/in/shillong',
    '/in/shimla',
    '/in/sholapur',
    '/in/silchar',
    '/in/srinagar',
    '/in/surat',
    '/in/tanjavur',
    '/in/tezpur',
    '/in/tezu',
    '/in/thiruvananthapuram',
    '/in/tiruchirappalli',
    '/in/tirupati',
    '/in/tuticorin',
    '/in/udaipur',
    '/in/vadodara',
    '/in/varanasi',
    '/in/vijayawada',
    '/in/visakhapatnam',
    '/in/warangal'
]

def start():
    c = MeshATC('rpc.pravah.io:5555')
    feed = c.subscribe(geospace)

    datalake = Datalake('pravah', 'Pr@v@h@dm!n', c.get_channel())

    for m, c in feed:
        jsonObj = json.loads(MessageToJson(m))
        obj = datalake.insert(c, jsonObj)
        print('[ATC] ' + str(obj) + ': ' + c)

if '__main__' == __name__:
    start()