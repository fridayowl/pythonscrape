import requests
from bs4 import BeautifulSoup
from datetime import datetime , timedelta
from scrapingbee import ScrapingBeeClient
import time
import csv
import boto3
import io
import json


verbose_file_name = 'verbose_temp1.csv'
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
        seconds = current_time[-2:]
        time_stamp = current_time
        return time_stamp



def verbose(data):  
    
    verbose_bucket = s3.Bucket(bucket_name)
    verbose_obj = verbose_bucket.Object(verbose_file_name)
    verbose_data.append(getTimestamp()+ ' >         ' + str(data) + '\n')
    # Write the updated data back to the S3 file
    body = 'Timestamp:\n' + '\n'.join(verbose_data)
    verbose_obj.put(Body=body)
def lambda_handler(event, context):
    print(getTimestamp())
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
    key = 'website3.csv'
    bucket_name = 'scrapecsvfile'
    url = "https://yofreesamples.com/courses/free-discounted-udemy-courses-list/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    
    verbose("Initiating web scraping...")
    verbose("Maximum Retires:"+json_data["max_retries"])
    verbose("Maximum Count:"+json_data["max_count"])
    verbose("Maximum Expiry:"+json_data["max_expiry"])
    verbose("WebsiteUrl:"+url)
    verbose("Connecting to the website...")
    verbose("Started Writing Files ")
    num_lines=0
     
    try:
        s3 = boto3.resource('s3')
        # Get a reference to the file in the bucket
        file_obj = s3.Object(bucket_name, key)
        
        # Read the contents of the file into a string
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        
        # Use the CSV module to count the number of rows in the file
        num_lines = sum(1 for line in csv.reader(io.StringIO(file_content)))
        response = requests.get(url, headers=headers) 
        soup = BeautifulSoup(response.content, "html.parser")
        print(soup)
        print(response)
        h1_tags = soup.find_all("h1", class_="entry-title")
        for h1 in h1_tags:
            print(h1.text.strip())
            date_string= h1.text.strip()
        
        date_str = date_string.split("–")[1].strip()
        
        # parse the date string using datetime.strptime()
        date_obj = datetime.strptime(date_str, "%m/%d/%Y")
        
        # print the resulting datetime object
        print(date_obj)
        now = datetime.now()
        delta = now - date_obj
        count=0
        # check if the time difference is less than 72 hours (3 days)
        if delta < timedelta(days=3):
            print("The date and time is less than 72 hours old.")
            divs = soup.find_all('div', {'class': 'kt-inside-inner-col'})
            # Extract the contents of the divs
            for div in divs:
                print(div.text)
                count+=1
        print(count)
        #     div_element = soup.find('div', {'class': 'wp-block-kadence-rowlayout'})
        #     for result in div_element:
        #         h4_tags = result.find_all('h4')
        #         for tag in h4_tags:
        #             print(tag.text)
            
        header = ['Course Name', 'Course URL', 'Coupon Code', 'Expiration Date']
        # # Open the file in write mode and write the header
        # with open('courses.csv', mode='w', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerow(header)
        bucket_name = 'scrapecsvfile'
        key = 'website3.csv'
        with open('/tmp/website1.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
        # Upload the file to S3
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file('/tmp/website1.csv', bucket_name, key)
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if 'udemy.com' in href and 'couponCode' in href:
                s3 = boto3.resource('s3')
                # Get a reference to the file in the bucket
                file_obj = s3.Object(bucket_name, "website3.csv")
                
                # Read the contents of the file into a string
                file_content = file_obj.get()['Body'].read().decode('utf-8')
                
                # Use the CSV module to count the number of rows in the file
                num_lines = sum(1 for line in csv.reader(io.StringIO(file_content)))
                fullurl=href
                verbose("Extracting information from HTML tags...")
                couponcode = href.split("couponCode=")[-1]
                print("couponcode")
                print(couponcode)
                client = ScrapingBeeClient(api_key='PTGMJINDWBZZDYNIT8LLXSFWL7H1EYCD1Q172E9VXZX716YQHMBE4EXNW53IMMLRBAQDIE1797DSK5Z1')
                response = client.get(fullurl)
                time.sleep(2)
                print('Response HTTP Status Code: ', response.status_code)
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
                    verbose("Checking if the course is Free on Coupon")
                    #print('Response HTTP Response Body: ', response.content)
                    soup = BeautifulSoup(response.content, 'html.parser') 
                    span_element = soup.find('span', class_='redeem-coupon--code-text--2HFA4')
                    coupon_status ="not applied"
                    if span_element and 'applied' in span_element.text:
                        #print('Text "applied" found in span element.')
                        coupon_status="Applied"
                    discount ="0%"
                    try:
                        html2 = soup.find('h1', class_='ud-heading-xl clp-lead__title clp-lead__title--small', attrs={'data-purpose': 'lead-title'})
                        soup2 = BeautifulSoup(html2, 'html.parser')
                        course_title = soup2.find('h1', class_='ud-heading-xl clp-lead__title clp-lead__title--small', attrs={'data-purpose': 'lead-title'}).text
                    except:
                        course_title  =fullurl.split('/')[-2].replace('-', ' ')
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
                    if coupon_status=="Applied" and discount == 100 :
                        verbose("It is Free only when the coupon applies")
                        print('Expiry date:', expiry_date)
                        if int(expiry_date) <= checkexpirydate :
                            today = datetime.now().date()
                            next_date = today + timedelta(days=int(expiry_date))
                            date_str = soup.find('div', {'class': 'last-update-date'}).text.split('Last updated ')[1]
                            last_update_date = datetime.strptime(date_str, '%m/%Y').date()
                            days_since_last_update = (datetime.now().date() - last_update_date).days
                            print(f'Last update date: {last_update_date}')
                            print(f'Days since last update: {days_since_last_update}')
                            print(next_date)
                            bucket_name = 'scrapecsvfile'
                            key = 'website2.csv'
                            print(couponcode, fullurl, couponcode, next_date)
                            bucket_name = 'scrapecsvfile'
                            key = 'website3.csv'
                            with open('/tmp/website1.csv', 'a') as f:
                                writer = csv.writer(f)
                                row=[course_title, fullurl, couponcode, next_date]
                                writer.writerow(row)
                            s3 = boto3.resource('s3')
                            s3.meta.client.upload_file('/tmp/website1.csv', bucket_name, key)
                            verbose("Storing the extracted information...")
                            verbose("Added data to CSV  ")
    except requests.exceptions.RequestException:
        retry_count += 1
        verbose("Request failed. Retrying")
        print(f"Request failed. Retrying ({retry_count}/{max_retries})...")
    return {
            'statusCode': 200,
            'body':json.dumps("Q")
        }
