import boto3
import json
from datetime import datetime
import time

my_stream_name = 'kinesis-test'

kinesis_client = boto3.client('dynamodbstreams', region_name='us-east-1')

response = kinesis_client.describe_stream(StreamArn="arn:aws:dynamodb:us-east-1:397929331209:table/MyDynamo/stream/2019-06-18T02:58:13.207")

my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
print(my_shard_id)
shard_iterator = kinesis_client.get_shard_iterator(StreamArn="arn:aws:dynamodb:us-east-1:397929331209:table/MyDynamo/stream/2019-06-18T02:58:13.207",
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='TRIM_HORIZON')

my_shard_iterator = shard_iterator['ShardIterator']
print(my_shard_iterator)
record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                              Limit=1000)
i = 1
while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                  Limit=1000)
    try:
        print(record_response["Records"][0]["dynamodb"]["NewImage"])
        s3 = boto3.resource('s3')
        obj = s3.Object('my-s3-bucket-for-san',str(i)+'.json')
        obj.put(Body=json.dumps(record_response["Records"][0]["dynamodb"]["NewImage"]))   
        i =i+1   
    except:
        import traceback
        print(traceback.format_exc())
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
    # wait for 5 seconds
    time.sleep(3)