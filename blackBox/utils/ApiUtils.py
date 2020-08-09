import boto3

def getDynamoHost(event):
    # TODO reevaluate this approach
    headers = event["headers"]
    host = "prod"
    if headers is not None:
        try:
            host = headers["Host"]
        except KeyError as error:
            host = "prod"

    if host == "localhost:3000" or host == "127.0.0.1:3000":
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url='http://dynamodb:8000')
        print("using localhost db") 
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        print("using prod db")
    return dynamodb