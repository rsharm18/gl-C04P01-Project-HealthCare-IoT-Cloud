import uuid


class Aggregated_Data_Model:
    def __init(self, device_type, avg, min, max, timerange):
        self._device_type = device_type
        self._average = avg
        self._minimum = min
        self._maximum = max
        self._timestamp = timerange
        self._id = uuid.uuid4()
