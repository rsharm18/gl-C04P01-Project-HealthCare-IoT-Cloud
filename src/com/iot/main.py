import json

from dateutil import parser

from src.com.iot.model.ConfigRuleDataModel import Config_Rule_Data_Model
from src.com.iot.service.Agreegator import Aggregator
from src.com.iot.service.AnomalyDetector import Anomaly_Detector
from src.com.iot.service.IProcessor import IProcessor
from src.com.iot.util import Database

Database.create_dynamodb_table("bsm_agg_data")
Database.create_dynamodb_table("bsm_alerts")

f = open("../../../config_rule.json", "r")
config_rule = json.load(f)
anomaly_data_rules = []
for rule in config_rule:
    avg_max = rule["avg_max"] if "avg_max" in rule else -99999999999
    avg_min = rule["avg_min"] if "avg_min" in rule else 99999999999
    anomaly_data_rules.append(
        Config_Rule_Data_Model(rule["id"], rule["rule_type"], rule["sensor_type"], avg_min, avg_max,
                               rule["trigger_count"]))

print("config_rules ", anomaly_data_rules)

from_date = parser.parse("2022-05-01 15:45:00")
to_date = parser.parse("2022-05-01 16:05:00")
aggregator: IProcessor = Aggregator(from_date, to_date)
aggregator.process()


anomaly_detector: IProcessor = Anomaly_Detector(anomaly_data_rules)
anomaly_detector.process()
