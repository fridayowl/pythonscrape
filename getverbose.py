import csv
import json
import boto3


s3 = boto3.resource('s3')
bucket_name = 'scrapecsvfile'

file_name = 'verbose_temp1.csv'

def lambda_handler(event, context):
    # Create an S3 client
    ty =  int(event["queryStringParameters"]['type']),
    if int(ty[0]) == 1: 
        file_name = 'verbose_temp1.csv'
    if int(ty[0]) == 3:
        file_name = 'verbose_temp3.csv'
    if int(ty[0]) == 2:
        file_name = 'verbose_temp2.csv'
    s3 = boto3.client('s3')
    
    # Download the CSV file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    csv_data = response['Body'].read().decode('utf-8')
    
    # Parse the CSV data using the csv module
    csv_reader = csv.DictReader(csv_data.splitlines())
    data = [row for row in csv_reader]
    
    # Convert the data to JSON format
    json_data = json.dumps(data)
    
    # Set the response headers and body
    headers = {'Content-Type': 'application/json'}
    sample ={
        "abc":ty[0]
    }
    body = json_data
    return {
        'statusCode': 200,
        'body': json_data,
        'headers': {
           "Content-Type" : "application/json",
            "Access-Control-Allow-Headers" : "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods" : "OPTIONS,POST",
            "Access-Control-Allow-Credentials" : True,
            "Access-Control-Allow-Origin" : "*",
            "X-Requested-With" : "*"
        }
    }
