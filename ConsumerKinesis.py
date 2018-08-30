import boto3
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

aws_access_key_id=''
aws_secret_access_key=''
my_stream_name = 'ESCM_EEL-ESCMOWNER-SC_HD'
region_name = 'ap-southeast-1'

kinesis_client = boto3.client('kinesis',aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,
                              region_name=region_name)

# str = file_path.rfind("/")
# consumer_stream_name = file_path[:str].replace('/','-')
# kinesis_client.delete_stream(StreamName=my_stream_name)
# streamsJson = kinesis_client.list_streams()
# streams = streamsJson['StreamNames']
# print(streams)

tries = 0
while True:
    tries += 1
    try:
        response = kinesis_client.describe_stream(StreamName=my_stream_name)
        # print(response)
        streamStatus = response['StreamDescription']['StreamStatus']
        if streamStatus == 'ACTIVE':
            # print("stream is active")
            shards = response['StreamDescription']['Shards']
            for shard in shards:
                shard_id = shard["ShardId"]
                # print (repr(shard))
                # print('=========shard_id: %s' %shard_id)
                shard_it =  kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                              ShardId=shard_id,
                                                              ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
                # print('=========shard_it: %s' %shard_it)LATEST TRIM_HORIZON
                while True:
                    out = kinesis_client.get_records(ShardIterator=shard_it, Limit=100)
                    # print('=========out: %s' %out)
                    if len(out) != 0:
                        print("======Records lenght: ",len(out["Records"]))
                        for o in out["Records"]:
                            data = o["Data"]
                            print('=========data: ',data)
                            # You specific data processing goes here
                    shard_it = out["NextShardIterator"]
    except:
        print('error while trying to describe kinesis stream : ', my_stream_name)