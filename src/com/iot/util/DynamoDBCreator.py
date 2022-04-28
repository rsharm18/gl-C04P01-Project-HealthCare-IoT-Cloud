import boto3


def create_table(table_name='bsm_data'):
    """
    Creates a DynamoDB table.

    :param table_name:
    :return: The newly created table.
    """
    dyn_resource = boto3.resource('dynamodb')

    params = {
        'TableName': table_name,
        'KeySchema': [
            {'AttributeName': 'deviceid', 'KeyType': 'HASH'},
            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'deviceid', 'AttributeType': 'S'},
            {'AttributeName': 'timestamp', 'AttributeType': 'S'}
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    table = dyn_resource.create_table(**params)
    print(f"Creating {table_name}...")
    table.wait_until_exists()
    print(f"Created table.")
    return table
