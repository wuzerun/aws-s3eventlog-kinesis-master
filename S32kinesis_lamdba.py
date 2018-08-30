import boto3
import time
import base64
from io import StringIO
import pandas
import csv


def lambda_handler(event, context):
    print('=========s3 to kinesis  event: %s' %event)
    s3_client = boto3.client('s3')

    kinesis_client = boto3.client('kinesis', region_name='ap-southeast-1')

    for record in event['Records']:
        eventVersion = record['eventVersion']
        # print('=========eventVersion: %s' %eventVersion)
        if eventVersion == '2.0':
            # print('=========starting get records=========')
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            filePath = 's3n://'+bucket+'/'+key
            print('=========bucket: %s' %bucket)
            print('=========key: %s' %key)
            print('=========filePath: %s' %filePath)

            str = key.rfind("/")
            consumer_stream_name = key[:str].replace('/','-')
            streamsJson = kinesis_client.list_streams()
            streams = streamsJson['StreamNames']

            df1 = pandas.read_csv(filepath,engine='python',encoding='UTF-8')#reading *.csv.gz
            df1.loc[:, 'SEQUENCE'] = time.strftime('%Y%m%d%H%M%S', time.localtime())#add a timestamp to the last field.

            stream = StringIO()
            df1.to_csv(stream)
            stream.seek(0)

            i = 0
            for row in csv.reader(stream):
                del row[0]
                partition_key=time.ctime()
                row = [i.replace('\n','\t').replace(',',' ') for i in row]#replace the '\n' and ',' in the field.
                line = ",".join(row)
                if i != 0:
                    line = line+str(i)#add a unique timestamp to each row.
                print(line)
                data = bytes(line,"utf-8")
                partition_key=time.ctime()
                print('=========data: ',data)
                if consumer_stream_name not in streams:
                    print("create new kinesis stream: " + consumer_stream_name)
                    kinesis_client.create_stream(StreamName=consumer_stream_name,ShardCount=1)
                    kinesis_client.put_record(StreamName=consumer_stream_name, Data=data, PartitionKey=partition_key)
                else:
                    kinesis_client.put_record(StreamName=consumer_stream_name, Data=data, PartitionKey=partition_key)
                i += 1

            # # Get, read, and split the file into lines
            # obj = s3_client.get_object(Bucket=bucket, Key=key)
            # # print('=========obj: %s' %obj)
            # body = obj['Body'].read()
            # # print('=========body: %s' %body)
            #
            # lines = body.splitlines()
            # print('=========lines: %s' %lines)
            #
            # # print('=========put records of %s  to kinesis stream' %key)
            # i = 0
            # for line in lines:
            #     partition_key=time.ctime()#get string now time
            #     if i == 0:
            #         data = bytes(line + ',' + 'SEQUENCE')
            #         print('=========data: %s' %data)
            #         if consumer_stream_name not in streams:
            #             print("create new kinesis stream: " + consumer_stream_name)
            #             kinesis_client.create_stream(StreamName=consumer_stream_name,ShardCount=1)
            #             kinesis_client.put_record(StreamName=consumer_stream_name, Data=data, PartitionKey=partition_key)
            #         else:
            #             kinesis_client.put_record(StreamName=consumer_stream_name, Data=data, PartitionKey=partition_key)
            #         i += 1
            #     else:
            #         seq = bytes(int(time.time() * 1000)) + bytes(i)
            #         # print('=========seq: %s' %seq)
            #         data = bytes(line + ',') + seq
            #         print('=========data: %s' %data)
            #         if consumer_stream_name not in streams:
            #             print("create new kinesis stream: " + consumer_stream_name)
            #             kinesis_client.create_stream(StreamName=consumer_stream_name,ShardCount=1)
            #             kinesis_client.put_record(StreamName=consumer_stream_name, Data=data, PartitionKey=partition_key)
            #         else:
            #             kinesis_client.put_record(StreamName=consumer_stream_name, Data=data, PartitionKey=partition_key)
            #         i += 1


    return 'Hello from Lambda'
