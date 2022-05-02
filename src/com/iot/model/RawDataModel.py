import datetime

import json


class Raw_Data_Model:
    def __init__(self, deviceid, datatype, value):
        self.deviceid = deviceid
        self.timestamp = str(datetime.datetime.now())
        self.datatype = datatype
        self.value = value

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)