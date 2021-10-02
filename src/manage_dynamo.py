import decimal
import pprint

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import boto3
import json
def get_dynamodb_client():
    dynamo_db = boto3.client("dynamodb", region_name='us-east-1')
    """ :type : pyboto3.dynamodb """
    return dynamo_db

def get_dynamo_resource():
    dynamo_db = boto3.resource("dynamodb", region_name='us-east-1')
    """ :type : pyboto3.dynamodb """
    return dynamo_db

def create_table():
    table = 'Movies'
    attribute_definitions =[
        {
            'AttributeName': 'year',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        }
    ]
    key_schema = [
        {
            'AttributeName': 'year',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'title',
            'KeyType': 'RANGE'
        }
    ]

    initial_iops = {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
    dynamo_table_response= get_dynamodb_client().create_table(
        TableName=table,
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        ProvisionedThroughput=initial_iops
    )
    print('created dynamo table:' + str(dynamo_table_response))

def put_items_in_table():
    try:
        response = get_dynamo_resource().Table("Movies").put_item(
            Item={
                'year': 2015,
                'title': 'Waddup the movie time',
                'info': {
                    'plot': 'How you doing ?',
                    'rating': decimal.Decimal(0)
                }
            }
        )
        print('A new movie added to collection successfully')
        print(str(response))
    except Exception as error:
        print(error)

def update_items_in_table():
    try:
        response = get_dynamo_resource().Table('Movies').update_item(
            Key={
                'year': 2015,
                'title': 'Waddup the movie time'
            },
            UpdateExpression="set info.rating = :r, info.plot=:p, info.actors=:a",
            ExpressionAttributeValues={
                ':r': decimal.Decimal(3.5),
                ':p': "Everything happens all at once.",
                ':a': ["David", "Alex", "Chiran"]
            },
            ReturnValues="UPDATED_NEW"
        )
        print("Update movie succeeded:")
        pprint.pprint(str(response))
    except Exception as error:
        print(error)

def conditionally_updates_in_table():
    try:
        response = get_dynamo_resource().Table('Movies').update_item(
            Key={
                'year':2015,
                'title': 'Waddup the movie time'
            },
            UpdateExpression="remove info.actors[0]",
            ConditionExpression="size(info.actors) >= :num",
            ExpressionAttributeValues={
                ':num': 3
            },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        print('updated table items conditionally')
        print(str(response))

def get_items_in_table():
    try:
        response = get_dynamo_resource().Table('Movies').get_item(
            Key={
                'year': 2015,
                'title': 'Waddup the movie time'
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print(str(response))



def delete_items_in_table_conditionally():
    try:
        response= get_dynamo_resource().Table("Movies").delete_item(
            Key={
                'year': 2015,
                'title': "Waddup the movie time"
            },
            ConditionExpression= "info.rating <= :val",
            ExpressionAttributeValues={
                ":val": decimal.Decimal(2)
            }
        )
    except  ClientError as error:
        if error.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(error.response['Error']['Message'])
        else:
            raise
    else:
        print("Deleted items successfully in the table")
        print(str(response))

def insert_sample_data():
    table = get_dynamo_resource().Table("Movies")
    with open('moviedata.json') as json_file:

        movies = json.load(json_file, parse_float=decimal.Decimal)
        for movie in movies:
            year = int(movie['year'])
            title = movie['title']
            info = movie['info']

            print("Adding movie:", year, title)
            table.put_item(
                Item={
                    'year': year,
                    'title': title,
                    'info': info
                }
            )
    print("Sample movies data added successfully!!")

def query_movies_in_19985():
    response = get_dynamo_resource().Table("Movies").query(
        KeyConditionExpression= Key('year').eq(2013)
    )
    for movie in response['Items']:
        print(movie['year'], ':', movie['title'])

def query_movies_with_extra_condition():
    response = get_dynamo_resource().Table("Movies").query(
        ProjectionExpression="#yr, title, info.genres, info.actors[0]",
        ExpressionAttributeNames= {"#yr":"year"},
        KeyConditionExpression= Key('year').eq(1985) & Key('title').between('A', 'L')

    )
    for movie in response['Items']:
        print(str(movie))

def scan_my_dynamo_table():
    filter_exp = Key('year').between(1980, 1982)
    projection_exp = "#yr, title, info.rating"
    exp_an ={"#yr":"year"}
    response= get_dynamo_resource().Table("Movies").scan(
        FilterExpression=filter_exp,
        ProjectionExpression=projection_exp,
        ExpressionAttributeNames=exp_an
    )
    for movie in response['Items']:
        print(str(movie))



if __name__ == '__main__':
    scan_my_dynamo_table()
    #query_movies_with_extra_condition()
    #query_movies_in_19985()
    #insert_sample_data()
    #delete_items_in_table_conditionally()
    #get_items_in_table()
    #update_items_in_table()
    #conditionally_updates_in_table()
    #create_table()
    #put_items_in_table()
    #update_items_in_table()

