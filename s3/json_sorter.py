import json
import boto3
from pathlib import Path
from urllib.parse import urlparse
import requests

print('Loading json sorter')

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # this code is a slight modification of
    # https://aws.amazon.com/blogs/aws/introducing-amazon-s3-object-lambda-use-your-code-to-process-data-as-it-is-being-retrieved-from-s3/
    # with a little dash of https://eoins.medium.com/using-s3-object-lambdas-to-generate-and-transform-on-the-fly-874b0f27fb84

    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    s3_url = object_get_context["inputS3Url"]

    requested_url = event['userRequest']['url']
    path = Path(urlparse(requested_url).path).relative_to('/')
    # Get object from S3
    response = requests.get(s3_url)
    original_object = response.content.decode('utf-8')

    if path.suffix == '.json':
        # sort in place
        data = json.loads(original_object)
        data.sort()
        returned_data = json.dumps(data)
    else:
        # not json, just pass through
        returned_data = original_object

    # Write object back to S3 Object Lambda
    s3.write_get_object_response(
        Body=returned_data,
        RequestRoute=request_route,
        RequestToken=request_token)

    return {'status_code': 200}

