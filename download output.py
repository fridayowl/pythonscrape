import boto3
import csv
from io import StringIO 
import json
def lambda_handler(event, context):
    
    # Define the S3 client
    s3 = boto3.client('s3')
    
    # Define the bucket name and the file names
    bucket_name = 'scrapecsvfile'
    file_names = ['website1.csv', 'website2.csv', 'website3.csv']
        # Initialize an empty list to store the merged rows
    merged_rows = []
    
    # Loop through the file names and read the CSV data
    for file_name in file_names:
        
        # Read the CSV data from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        csv_data = response['Body'].read().decode('utf-8')
        
        # Parse the CSV data into a list of rows
        csv_reader = csv.reader(StringIO(csv_data))
        csv_rows = list(csv_reader)
        
        # Skip the header row if this is not the first file
        if len(merged_rows) > 0:
            csv_rows.pop(0)
        
        # Append the rows to the merged list
        merged_rows += csv_rows
    
    # Remove any duplicated headers
    headers = merged_rows.pop(0)
    deduped_headers = list(dict.fromkeys(headers))
    
    # Initialize an empty string to store the merged CSV data
    merged_csv_data = StringIO()
    
    # Write the headers to the merged CSV data
    merged_csv_writer = csv.writer(merged_csv_data)
    merged_csv_writer.writerow(deduped_headers)
    
    # Write the rows to the merged CSV data
    merged_csv_writer.writerows(merged_rows)
    
    # Upload the merged file to S3
    s3.put_object(Body=merged_csv_data.getvalue().encode('utf-8'), Bucket=bucket_name, Key='merged.csv')
    # Set the object ACL to public-read
    s3.put_object_acl(Bucket=bucket_name, Key='merged.csv', ACL='public-read')
    
    # Generate the URL for the file
    url = f"https://{bucket_name}.s3.amazonaws.com/merged.csv"
    response = s3.head_object(Bucket=bucket_name, Key='merged.csv')
    file_size = response['ContentLength']
    # Convert the file size to KB or MB
    if file_size < 1024:
        size_str = f"{file_size} B"
    elif file_size < 1024 ** 2:
        size_str = f"{file_size / 1024:.2f} KB"
    else:
        size_str = f"{file_size / (1024 ** 2):.2f} MB"
    last_modified = response['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
    # Return the response body
    
    body = {
        'url': url,
        'size': size_str,
        'last_modified': last_modified
    }
    
    return {
        'statusCode': 200,
        'body':json.dumps( body),
        'headers': {
           "Content-Type" : "application/json",
            "Access-Control-Allow-Headers" : "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods" : "OPTIONS,POST",
            "Access-Control-Allow-Credentials" : True,
            "Access-Control-Allow-Origin" : "*",
            "X-Requested-With" : "*"
        }
    }
