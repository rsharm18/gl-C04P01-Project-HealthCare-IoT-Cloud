import boto3
from boto3.dynamodb.conditions import Key
from dateutil import parser

from src.com.iot.util import DynamoDBCreator


def get_registered_devices():
    return ["BSM_01", "BSM_02","BSM_03"]

def marshall_data(obj):
    data = {}
    for variable, value in vars(obj).items():
        data[variable] = value

    # print("data ",data)
    return data

def create_dynamodb_table(tableName):
    DynamoDBCreator.create_table(tableName)


def is_date_in_range(value, from_date, to_date):
    parsed_date = parser.parse(value)

    return from_date < parsed_date < to_date


class Database:
    def __init__(self, table_name):
        self._table_name = table_name
        self._dynamodb = boto3.resource('dynamodb')
        self._table = self._dynamodb.Table(table_name)


    def insert_data(self, data):
        self._table.put_item(Item=data)

    def get_device_raw_data(self, from_date, to_date):
        # f = open("../../../raw_data.json", "r")
        # data = json.load(f)
        # #
        # # database = boto3.resource('dynamodb')
        # # table = database.Table("bsm_data")
        # # table.put_item(Item=data)
        # # for item in data:
        # #     table.put_item(Item=data)
        # return data

        # dynamodb_client = boto3.client('dynamodb', region_name="us-east-1")
        # data = dynamodb_client.query(
        #     TableName="bsm_data",
        #     ExpressionAttributeNames='#tS = timeStamp',
        #     KeyConditionExpression='#ts BETWEEN from_date and to_date',
        #     ExpressionAttributeValues={
        #         ':from_date': {'S': from_date},
        #         ':to_date': {'S': to_date}
        #     }
        # )

        bsm_data_table = boto3.resource('dynamodb')
        fe = Key('timeStamp').between(from_date, to_date)
        # attr = bsm_data_table.conditions.Attr('timeStamp')
        response = bsm_data_table.Table("bsm_data").scan()

        items = response["Items"]
        # print("items before sort", items)

        # items.sort(key=lambda x: parser.parse(x["timestamp"]))
        # # print("items after sort", items)
        # # print("\n\n")
        # return self.apply_date_range(response["Items"], from_date, to_date)
        #
        return response["Items"]

    def get_data_from_table(self,table_name):
        bsm_data_table = boto3.resource('dynamodb')
        # attr = bsm_data_table.conditions.Attr('timeStamp')
        response = bsm_data_table.Table(table_name).scan()

        return response["Items"]

    def apply_date_range(self, items, from_date, to_date):
        filteredList: list = []
        for item in items:
            if is_date_in_range(item['timestamp'], from_date, to_date):
                filteredList.append(item)
                print(item)

        return filteredList

    def get_data_by_range(self, from_date, to_Date):
        response = ""
        # attr = self._dynamodb.conditions.Attr('timeStamp')
        # response = self._table.scan(
        #     FilterExpression=attr.between(from_date, to_Date)
        # )

        return response
    #
    # def insert_data(self, item):
    #     response = self._table.put_item(
    #         Item=item
    #     )
    #     print(response)
