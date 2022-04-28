from src.com.iot.util import Database

Database.create_dynamodb_table("bsm_data")
Database.create_dynamodb_table("bsm_agg_data")
Database.create_dynamodb_table("bsm_alerts")

