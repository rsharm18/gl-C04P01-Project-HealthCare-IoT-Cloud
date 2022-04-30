from datetime import timedelta
from typing import List

from src.com.iot.service.IProcessor import IProcessor
from src.com.iot.util.Database import Database


class Aggregator(IProcessor):
    def __init__(self, from_date, to_date, time_interval="60"):
        self._time_interval = time_interval
        self._from_date = from_date
        self._to_date = to_date
        self._database = Database("bsm_agg_data")

    @property
    def databse(self):
        return self._database

    def process(self):
        print("I am ready to process the data")
        raw_data_set: List = self.databse.get_device_raw_data(self._from_date, self._to_date)

        grouped_data = self.aggregate_data_by_sensor(raw_data_set)
        print("raw data is {0}".format(grouped_data))

    def aggregate_data_by_sensor(self, items):
        group_data = {'HeartRate': [], 'Temperature': []}
        for item in items:
            group_data[item["datatype"]].append(item)

        final_time = self._from_date
        for min in range(1, 61):
            final_time = final_time + timedelta(minutes=1)
            print(min)
            print(final_time)

        return group_data

    def group_sensor_data_by_minutes(self, sensor_type, sensor_data_list):
        grouped_data_by_minute = {}

        for min in range(1, 60):
            print(min)

        sensor_data_by_minute = {sensor_type: grouped_data_by_minute}

        return sensor_data_by_minute
