import boto3
import gzip
import os
from io import StringIO
from io import BytesIO
import pandas
import io
import s3fs
import os
from s3fs.core import S3FileSystem
import time
import csv

aws_access_key_id=''
aws_secret_access_key=''
os.environ["AWS_ACCESS_KEY_ID"] = aws_secret_access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

region_name = 'ap-southeast-1'
my_stream_name = 'ESCM_EEL-ESCMOWNER-SC_HD'
filepath ="s3n://esq-dms-staging-prd/ESCM_EEL/ESCMOWNER/SC_HD/20180828-045045550.csv.gz"

kinesis_client = boto3.client('kinesis',aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,
                              region_name=region_name)

# s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id,
#                          aws_secret_access_key=aws_secret_access_key,
#                          region_name=region_name)
# bucket='esq-dms-staging-prd'
# key='ESCM_EEL/ESCMOWNER/BOM_HD/20180820-092200426.csv.gz'
# obj = s3_client.get_object(Bucket=bucket, Key=key)
# print(obj)
# stream_body = obj['Body'].read(amt = None)#.decode("utf-8")
# print(stream_body)
# gzip1 = BytesIO(stream_body)
# gzipfile = gzip.GzipFile(fileobj=gzip1, mode='rb')
# content = gzipfile.read()
# print(content)

df1 = pandas.read_csv(filepath,engine='python',encoding='UTF-8')
df1.loc[:, 'SEQUENCE'] = time.strftime('%Y%m%d%H%M%S', time.localtime())#add a timestamp to the last field.

stream = StringIO()
df1.to_csv(stream)
stream.seek(0)

i=0
for row in csv.reader(stream):
    del row[0]#delete first row
    row = [j.replace('\n','\t').replace(',',' ') for j in row]#replace the '\n' and ',' in the field.
    line = ",".join(row)#list convert stream
    if i != 0:
        line = line+str(i)#add a unique timestamp to each row.
    print(line)
    data = bytes(line,"utf-8")
    partition_key=time.ctime()
    kinesis_client.put_record(StreamName=my_stream_name, Data=data, PartitionKey=partition_key)
    i += 1