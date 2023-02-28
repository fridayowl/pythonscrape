import json
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from scrapingbee import ScrapingBeeClient
from bs4 import BeautifulSoup
import time
import csv
import boto3
import io
import json 




verbose_file_name = 'verbose_temp3.csv'
bucket_name = 'scrapecsvfile'
verbose_data=[]
s3 = boto3.resource('s3')
file_obj = s3.Object(bucket_name, verbose_file_name)
# Clear the file
file_obj.put(Body='')

def getTimestamp():
        import datetime
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")
        seconds = str(now.second)
        time_stamp = current_time 
        return time_stamp



def verbose(data):  
    verbose_bucket = s3.Bucket(bucket_name)
    verbose_obj = verbose_bucket.Object(verbose_file_name)
    verbose_data.append(getTimestamp()+ ' > ' + str(data))
    # Write the updated data back to the S3 file
    body = 'Timestamp:\n' + '\n'.join(verbose_data)
    verbose_obj.put(Body=body)


def lambda_handler(event, context):

    # Return the result as an HTTP response
    s3 = boto3.resource('s3')
    bucket_name = 'scrapecsvfile'
    obj = s3.Object(bucket_name, "parameters.json")
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    print(json_data)
    max_retries = int(json_data["max_retries"]  )
    retry_count =0
    checkexpirydate=int(json_data["max_expiry"]  )  
    maxcount=int(json_data['max_count']    ) 
    url = 'https://freesoff.com/t/100-off-udemy-course-28-02-2023/404971'
    header = ['Course Name', 'Course URL', 'Coupon Code', 'Expiration Date']
    bucket_name = 'scrapecsvfile'
    key = 'website3.csv'
    verbose("Maximum Retires:"+json_data["max_retries"])
    verbose("Maximum Count:"+json_data["max_count"])
    verbose("Maximum Expiry:"+json_data["max_expiry"])
    verbose("Initiating web scraping...")
    verbose("WebsiteUrl"+'https://freesoff.com/t/100-off-udemy-course-24-02-2023/403859')
    with open('/tmp/website3.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
    # Upload the file to S3
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('/tmp/website3.csv', bucket_name, key)
    s3.meta.client.upload_file('/tmp/website3.csv', bucket_name, "website3.csv")
    verbose("Connecting to the website...")
    verbose("Started Writing Files ")
    # Parse the HTML content of the webpage using BeautifulSoup
    response = requests.get(url) 
    soup = BeautifulSoup(response.content, 'html.parser')
    #_______________________________
    verbose("Retrieving the web page...")
    verbose("Parsing the HTML...")
    #__________________________
    num_lines=0
    verbose("RetryCount:"+str(retry_count))
    try :
            verbose("Locating HTML tags...")
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                verbose("Extracting information from HTML tags...")
                if 'udemy.com' in href and 'couponCode' in href:
                    s3 = boto3.resource('s3')
                    # Get a reference to the file in the bucket
                    file_obj = s3.Object(bucket_name, "website3.csv")
                    # Read the contents of the file into a string
                    file_content = file_obj.get()['Body'].read().decode('utf-8')
                    # Use the CSV module to count the number of rows in the file
                    num_lines = sum(1 for line in csv.reader(io.StringIO(file_content)))
                    print(href)
                    fullurl=href
                    couponcode = href.split("couponCode=")[-1]
                    print("couponcode")
                    print(couponcode)
                    client = ScrapingBeeClient(api_key='PTGMJINDWBZZDYNIT8LLXSFWL7H1EYCD1Q172E9VXZX716YQHMBE4EXNW53IMMLRBAQDIE1797DSK5Z1')
                    response = client.get(fullurl)
                    time.sleep(2)
                    print('Response HTTP Status Code: ', response.status_code)
                    verbose(str(response.status_code))
                    if num_lines >= maxcount:
                        verbose("Count Reached Max...")
                        break
                    if retry_count > max_retries:
                        verbose("Retry Exceeded...")
                        break
                    if(response.status_code == 401 ):
                        verbose("Api Limit Exceeds! ")
                        break
                    else:
                        #print('Response HTTP Response Body: ', response.content)
                        soup = BeautifulSoup(response.content, 'html.parser') 
                        try:
                            html2 = soup.find('h1', class_='ud-heading-xl clp-lead__title clp-lead__title--small', attrs={'data-purpose': 'lead-title'})
                            soup2 = BeautifulSoup(html2, 'html.parser')
                            course_title = soup2.find('h1', class_='ud-heading-xl clp-lead__title clp-lead__title--small', attrs={'data-purpose': 'lead-title'}).text
                        except:
                            course_title  =fullurl.split('/')[-2].replace('-', ' ')
                        span_element = soup.find('span', class_='redeem-coupon--code-text--2HFA4')
                        coupon_status ="not applied"
                        if span_element and 'applied' in span_element.text:
                            #print('Text "applied" found in span element.')
                            coupon_status="Applied"
                        discount ="0%"
                        try:
                            discount_percent = soup.find('div', {'data-purpose': 'discount-percentage'}).get_text().strip()
                        except :
                            discount_percent="0%"  
                        #print(discount_percent)
                        discount=0;
                        if "100%" in discount_percent:
                            #print("Discount percentage contains 100%")
                            discount=100 
                        #print(original_price)
                        try:
                            expiry_date = soup.find('div', {'data-purpose': 'discount-expiration'}).text.strip().split()[0]
                            #print(expiry_date)
                        except :
                            expiry_date = checkexpirydate
                        print(coupon_status)
                        print(discount)
                        verbose("Checking if the course is Free on Coupon")
                        if coupon_status=="Applied" and discount == 100 :
                            verbose("It is Free only when the coupon applies")
                            print('Expiry date:', expiry_date)
                            if int(expiry_date ) <= checkexpirydate :
                                verbose("Checking expiry data")
                                today = datetime.now().date()
                                next_date = today + timedelta(days=int(expiry_date))
                                date_str = soup.find('div', {'class': 'last-update-date'}).text.split('Last updated ')[1]
                                last_update_date = datetime.strptime(date_str, '%m/%Y').date()
                                days_since_last_update = (datetime.now().date() - last_update_date).days
                                print(f'Last update date: {last_update_date}')
                                print(f'Days since last update: {days_since_last_update}')
                                print(next_date)
                                bucket_name = 'scrapecsvfile'
                                key = 'website3.csv'
                                print(couponcode, fullurl, couponcode, next_date)
                                bucket_name = 'scrapecsvfile'
                                key = 'website3.csv'
                                with open('/tmp/website3.csv', 'a') as f:
                                    writer = csv.writer(f)
                                    row=[course_title, fullurl, couponcode, next_date]
                                    writer.writerow(row)
                                s3 = boto3.resource('s3')
                                s3.meta.client.upload_file('/tmp/website3.csv', bucket_name, key)
                                verbose("Storing the extracted information...")
                                verbose("Added data to CSV  ")
                        else:
                            verbose("Data did not match the condition!!")
    except requests.exceptions.RequestException:
            retry_count += 1
            verbose("Request failed. Retrying")
            print(f"Request failed. Retrying ({retry_count}/{max_retries})...")

    return {
        'statusCode': 200,
        'body': json.dumps("s"),
        'headers': {
           "Content-Type" : "application/json",
            "Access-Control-Allow-Headers" : "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods" : "OPTIONS,POST",
            "Access-Control-Allow-Credentials" : True,
            "Access-Control-Allow-Origin" : "*",
            "X-Requested-With" : "*"
        }
    }
