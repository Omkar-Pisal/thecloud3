import boto3
import time
import os

os.environ['AWS_ACCESS_KEY_ID'] = ''
os.environ['AWS_SECRET_ACCESS_KEY'] = ''

# Set up S3 client
s3 = boto3.client('s3',region_name='us-east-1')

# Set up Lambda client
lambda_client = boto3.client('lambda',region_name='us-east-1')

# Set up input and output bucket names
input_bucket = 's3-input-bucket-project3'
output_bucket = 's3-output-bucket-project3'

# Set up Lambda function name
function_name = 'your-lambda-function-name'

# Continuously monitor input bucket for new videos
while True:
    response = s3.list_objects_v2(Bucket=input_bucket)

    if 'Contents' in response:
        for item in response['Contents']:
            # Get the video key and process the video
            video_key = item['Key']
            lambda_client.invoke(FunctionName=function_name, Payload='{"input_key": "' + video_key + '", "output_bucket": "' + output_bucket + '"}')

            # Delete the processed video from input bucket
            # s3.delete_object(Bucket=input_bucket, Key=video_key)

    # Sleep for 5 seconds before checking for new videos again
    time.sleep(5)

    # Continuously monitor output bucket for academic information
    response = s3.list_objects_v2(Bucket=output_bucket)

    if 'Contents' in response:
        for item in response['Contents']:
            # Get the academic information key and print/save it
            academic_info_key = item['Key']
            academic_info_object = s3.get_object(Bucket=output_bucket, Key=academic_info_key)
            academic_info = academic_info_object['Body'].read().decode('utf-8')

            # Print the academic information on CLI or save it in a file
            print(academic_info)

            # Delete the academic information object from output bucket
            # s3.delete_object(Bucket=output_bucket, Key=academic_info_key)

    # Sleep for 5 seconds before checking for new academic information again
    time.sleep(5)