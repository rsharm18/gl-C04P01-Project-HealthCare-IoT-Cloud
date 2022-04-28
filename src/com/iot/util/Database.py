import boto3
from boto3 import dynamodb

from src.com.iot.util import DynamoDBCreator


def create_dynamodb_table(tableName):
    DynamoDBCreator.create_table(tableName)


class Database:



    def __init__(self, tableName):
        self._table_name = tableName
        self._dynamodb = boto3.resource('dynamodb')
        self._table = self._dynamodb.Table('table_name')

    def get_data_by_range(self,from_date,to_Date):
        attr = self._dynamodb.conditions.Attr('timeStamp')
        response = self._table.scan(
            FilterExpression=attr.between(from_date, to_Date)
        )

        return response
