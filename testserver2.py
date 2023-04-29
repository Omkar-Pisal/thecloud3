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

inputSQS_url = "https://sqs.us-east-1.amazonaws.com/"
outputSQS_url = "https://sqs.us-east-1.amazonaws.com/"

# Set up input and output bucket names
input_bucket = 'cloudy-input-bucket-project2'
output_bucket = 'cloudy-output-bucket-project2'

# Set up Lambda function name
function_name = 'project_2'

def handle_output_message(msg):
    outputmsg_body = msg['Body']
    data = json.loads(outputmsg_body)
    key = data['Records'][0]['s3']['object']['key']
    academic_info_object = s3.get_object(Bucket=output_bucket, Key=key)
    academic_info = academic_info_object['Body'].read().decode('utf-8')
    print(key+": "+academic_info)
    sqs_client.delete_message(QueueUrl=outputSQS_url, ReceiptHandle=msg['ReceiptHandle'])

def handle_input_message(msg):
    msg_body = msg['Body']
    lambda_client.invoke(FunctionName=function_name, Payload=msg_body)
    sqs_client.delete_message(QueueUrl=inputSQS_url,ReceiptHandle=msg['ReceiptHandle'])

def handle_messages():
    while True:
        input_messages = sqs_client.receive_message(QueueUrl=inputSQS_url, MaxNumberOfMessages=10, MessageAttributeNames=['All'])
        output_messages = sqs_client.receive_message(QueueUrl=outputSQS_url, MaxNumberOfMessages=10, MessageAttributeNames=["All"])

        input_threads = []
        for msg in input_messages.get('Messages', []):
            t = threading.Thread(target=handle_input_message, args=(msg,))
            input_threads.append(t)
            t.start()

        output_threads = []
        for msg in output_messages.get('Messages', []):
            t = threading.Thread(target=handle_output_message, args=(msg,))
            output_threads.append(t)
            t.start()

        for t in input_threads:
            t.join()

        for t in output_threads:
            t.join()

        time.sleep(1)

def main():
    print("Program started")
    # Continuously monitor input and output SQS queues
    handle_messages()

if __name__ == '__main__':
    main()
