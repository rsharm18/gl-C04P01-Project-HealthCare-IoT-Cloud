from datetime import timedelta
from typing import List

from dateutil import parser

from src.com.iot.model.AggregatedDataModel import Aggregated_Data_Model
from src.com.iot.service.IProcessor import IProcessor
from src.com.iot.util.Database import Database, marshall_data, get_registered_devices


class Aggregator(IProcessor):
    def __init__(self, from_date, to_date, time_interval="60"):
        self._time_interval = time_interval
        self._from_date = from_date
        self._to_date = to_date
        self._database = Database("bsm_agg_data")
        self.existing_device_ids = get_registered_devices()

    @property
    def databse(self):
        return self._database

    def process(self):
        # read raw data
        raw_data_set: List = self.databse.get_device_raw_data(self._from_date, self._to_date)

        # filter data by device id
        for device_id in self.existing_device_ids:
            print("Aggregating data for device {0}".format(device_id))
            self.aggregate_device_data_for_device(device_id, raw_data_set)
        # agreegate data for each device
        # grouped_data = self.aggregate_data_by_sensor(raw_data_set)
        # print("raw data is {0}".format(grouped_data))

    def aggregate_device_data_for_device(self, deviceid, items: List):
        device_data = list(filter(
            lambda item: item["deviceid"] == deviceid and self.is_date_in_range(item["timestamp"], self._from_date,
                                                                                self._to_date), items))
        device_data.sort(key=lambda x: parser.parse(x["timestamp"]))
        start_time = self._from_date

        sensors = ["HeartRate", "SPO2", "Temperature"]
        aggregated_data = []
        while start_time < self._to_date:
            end_time = start_time + timedelta(minutes=1)
            # print("Range :: ",start_time," - ",end_time)
            # aggregate data by start and end date
            for sensor_type in sensors:

                filtered_data_set: list = list(filter(
                    lambda item: item["datatype"] == sensor_type and self.is_date_in_range(item["timestamp"],
                                                                                           start_time,
                                                                                           end_time), device_data))
                # print(" sensor_type :: ", sensor_type, " len(filtered_data_set) ", len(filtered_data_set))
                if len(filtered_data_set) > 0:
                    # print("aggregate data from {0} and {1}, data = {2}".format(start_time, end_time, filtered_data_set))

                    minm = float('inf')
                    maxm = float('-inf')
                    total = 0
                    for item in filtered_data_set:
                        value = float(item["value"])
                        if value > maxm:
                            maxm = value

                        if value < minm:
                            minm = value

                        total += value

                    avg = total / len(filtered_data_set)

                    agdm = Aggregated_Data_Model(deviceid, sensor_type, self.format_decimal_digits(avg),
                                                 self.format_decimal_digits(minm), self.format_decimal_digits(maxm),
                                                 start_time, end_time)

                    self._database.insert_data(marshall_data(agdm))
                    aggregated_data.append(marshall_data(agdm))
                    # print(" sensor_type :: ", sensor_type, "len device_data ", len(filtered_data_set), " len aggregated data ", len(aggregated_data))

            start_time = end_time

        # print("Data Count for all sensors ", len(device_data), " aggregated data size ", len(aggregated_data))
        # print("End aggregating data for :: ", deviceid)
        # print("deviceId ", deviceid)
        # print("device_data ", device_data)
        # print("aggregated data ", aggregated_data)

    def format_decimal_digits(self, value):
        return '{0:.3g}'.format(value)

    def aggregate_sensor_data(self, sensor_type, data_set, start_time, end_time):
        filtered_data_set = filter(
            lambda item: item["datatype"] == sensor_type and self.is_date_in_range(item["timestamp"], start_time,
                                                                                   end_time), data_set)

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

    def is_date_in_range(self, value, from_date, to_date):
        parsed_date = parser.parse(value)
        return from_date < parsed_date < to_date
