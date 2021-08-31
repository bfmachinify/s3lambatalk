import json
import urllib.parse
import boto3

print('Loading upload sorter')

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file_object = s3.Object(bucket, key).get()
    file_data = file_object['Body'].read()
    contents = file_data.decode('utf-8')
    first_char = contents[0]
    new_path = f'contains_{first_char}/{key}'
    s3.Object(bucket, new_path).copy_from(CopySource={'Bucket': bucket, 'Key': key})
   # s3.Object(bucket, key).delete()
    return new_path
