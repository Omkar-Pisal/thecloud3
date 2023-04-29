import boto3
import time
import os
import urllib.parse

os.environ['AWS_ACCESS_KEY_ID'] = ''
os.environ['AWS_SECRET_ACCESS_KEY'] = ''

# Set up S3 client
s3 = boto3.client('s3', region_name='us-east-1')

# Set up Lambda client
lambda_client = boto3.client('lambda', region_name='us-east-1')

sqs_client = boto3.client('sqs', region_name="us-east-1")

inputSQS_url = ""
outputSQS_url = ""

# Set up input and output bucket names
input_bucket = 'cloudy-input-bucket-project2'
output_bucket = 'cloudy-output-bucket-project2'

# Set up Lambda function name
function_name = 'project_2'

# Continuously monitor input bucket for new videos
while True:
    messages = sqs_client.receive_message(QueueUrl=inputSQS_url, MaxNumberOfMessages=10, MessageAttributeNames=['All'])

    if 'Messages' in messages:
        for msg in messages['Messages']:
            msg_body = msg['Body']
            lambda_client.invoke(FunctionName=function_name,Payload=msg_body)

    time.sleep(5)

    outputmessages = sqs_client.receive_message(QueueUrl=outputSQS_url,MaxNumberOfMessages=10,MessageAttributeName=["All"])

    if 'Messages' in outputmessages:
        for outmsg in outputmessages['Messages']:
            outputmsg_body = outmsg['Body']
            key = urllib.parse.unquote_plus(
                outputmsg_body['Records'][0]['s3']['object']['key'],
                encoding='utf-8')
            academic_info_object = s3.get_object(Bucket=output_bucket, Key=key)
            academic_info = academic_info_object['Body'].read().decode('utf-8')

            print(academic_info)

    time.sleep(5)

    time.sleep(5)
