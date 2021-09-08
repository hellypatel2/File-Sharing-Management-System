import json
import urllib.parse
import boto3
from io import BytesIO
import gzip

s3 = boto3.client('s3')


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    if key.endswith(".gz"):
        return
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    compressed_body = BytesIO(gzip.compress(response["Body"].read()))
    s3.put_object(Bucket=bucket, Key=key+".gz", Body=compressed_body, ContentType="application/gzip")
    s3.delete_object(Bucket=bucket, Key=key)