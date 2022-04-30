from datetime import datetime

from dateutil import parser

from src.com.iot.service.Agreegator import Aggregator
from src.com.iot.service.IProcessor import IProcessor
from src.com.iot.util import Database

Database.create_dynamodb_table("bsm_agg_data")
Database.create_dynamodb_table("bsm_alerts")
# from_date = '2022-04-28 18:00:00'#parser.parse("2022-04-28 18:00:00")
# to_date = '2022-04-28 19:49:00'#parser.parse("2022-04-28 19:49:00")
from_date = parser.parse("2022-04-28 18:00:00")
to_date = parser.parse("2022-04-28 19:49:00")
aggregator: IProcessor = Aggregator(from_date,to_date)
aggregator.process()
