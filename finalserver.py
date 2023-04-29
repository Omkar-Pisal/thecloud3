import boto3
import time
import os
import json
import threading

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

def handle_message(msg):
    msg_body = msg['Body']
    lambda_client.invoke(FunctionName=function_name, Payload=msg_body)

    while True:
        output_messages = sqs_client.receive_message(QueueUrl=outputSQS_url, MaxNumberOfMessages=10, MessageAttributeName=["All"])

        if 'Messages' in output_messages:
            for outmsg in output_messages['Messages']:
                outputmsg_body = outmsg['Body']
                data = json.loads(outputmsg_body)
                key = data['Records'][0]['s3']['object']['key']
                print(key)
                academic_info_object = s3.get_object(Bucket=output_bucket, Key=key)
                academic_info = academic_info_object['Body'].read().decode('utf-8')
                print(academic_info)
                sqs_client.delete_message(QueueUrl=outputSQS_url, ReceiptHandle=outmsg['ReceiptHandle'])

        time.sleep(1)

def main():
    print("Program started")
    # Continuously monitor input bucket for new videos
    while True:
        messages = sqs_client.receive_message(QueueUrl=inputSQS_url, MaxNumberOfMessages=10, MessageAttributeNames=['All'])

        if 'Messages' in messages:
            threads = []
            for msg in messages['Messages']:
                t = threading.Thread(target=handle_message, args=(msg,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

        time.sleep(1)

if __name__ == '__main__':
    main()
