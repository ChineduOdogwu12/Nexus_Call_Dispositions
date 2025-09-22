#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Nexus Call Dispositions Sampled Today
import smtplib
import re
import io
from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pandas as pd
import s3fs

# Setup Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Database connection details
jdbc_url = "jdbc:postgresql://xxx.xxx.xxx.rds.amazonaws.com:xxx/xxx"
connection_properties = {
    "user": "xxx",
    "password": "xxx",
    "driver": "xxx"
}

# Corrected SQL query without extra closing parenthesis
query = """
    (SELECT *
       FROM xxx.xxx
       WHERE start >= '2024-07-01'
       AND country = 'NIGERIA'
       AND sample_status = 'Sampled'
    ) AS nexus_data
"""
# Read data from PostgreSQL using Spark JDBC
nexus_data = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Convert Spark DataFrame to Pandas DataFrame if needed
nexus_data = nexus_data.toPandas()

from datetime import date

# Assuming `Today` refers to the current date in 'YYYY-MM-DD' format
today_date = date.today().strftime('%Y-%m-%d')

# Corrected SQL query
query = f"""
    (SELECT *
       FROM xxx.xxx
       WHERE processing_date = '{today_date}'
       AND country = 'NIGERIA'
    ) AS sample_loaded_to_nexus_today
"""

# Read data from PostgreSQL using Spark JDBC
sample_loaded_to_nexus_today = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Convert Spark DataFrame to Pandas DataFrame if needed
sample_loaded_to_nexus_today = sample_loaded_to_nexus_today.toPandas()

# Calculate the total of "DOUBTFUL VERIFIED" and "FULLY VERIFIED"
total_loaded = len(nexus_data)

# Count occurrences of "FULLY VERIFIED" in the verified_cc column
total_fullyverified = sum(nexus_data['verified_cc'] == "FULLY VERIFIED")

# Calculate the total of "DOUBTFUL VERIFIED" and "FULLY VERIFIED"
total_doubtfulverified = sum(nexus_data['verified_cc'] == "DOUBTFUL VERIFIED")

# # Define the base S3 path and file name
# base_s3_path = "s3://xxx/Nexus_Exports_Nigeria_Doubtful_Verifications/"
# file_name = "Nexus_Exports_Nigeria_Doubtful_Verifications.xlsx"

# # Assuming nexus_data is your DataFrame
# # Save the final DataFrame to Excel in S3
# s3_path = f"{base_s3_path}{file_name}"

# # Write the DataFrame to Excel format and save it to the S3 bucket
# nexus_data.to_excel(s3_path, index=False, engine='xlsxwriter')

# print(f"File saved to S3: {s3_path}")
import pandas as pd

# Assuming nexus_data and sample_loaded_to_nexus_today are Pandas DataFrames
# Combine the data into a multi-sheet Excel file
s3_path = f"{base_s3_path}{file_name}"

# Save both DataFrames as separate sheets in the Excel file
with pd.ExcelWriter("local_file.xlsx", engine="xlsxwriter") as writer:
    nexus_data.to_excel(writer, sheet_name="Nexus Data", index=False)
    sample_loaded_to_nexus_today.to_excel(writer, sheet_name="Sample Loaded Today", index=False)

# Upload the file to S3
import boto3
s3 = boto3.client("s3")

# Extract bucket name and key from the S3 path
bucket_name = base_s3_path.split("/")[2]
key = "/".join(base_s3_path.split("/")[3:]) + file_name

# Upload the local file to S3
s3.upload_file("local_file.xlsx", bucket_name, key)

print(f"File saved to S3: {s3_path}")


# Initialize S3 client
s3 = boto3.client('s3')

# List objects in the specified S3 path
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

# Check if the file is listed in the response
if 'Contents' in response:
    files_found = False
    for obj in response['Contents']:
        print(obj['Key'])
        if obj['Key'] == f"{folder_path}/{file_name}":
            files_found = True
            print(f"File {file_name} exists in the bucket {bucket_name}.")
    if not files_found:
        print(f"Error: The file {file_name} does not exist in the bucket {bucket_name}.")
else:
    print(f"No files found in the specified S3 path: {folder_path}.")



#Email Sending
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from datetime import datetime

# AWS S3 configuration
S3_BUCKET = 'xxx'
S3_KEY = 'Nexus_Exports_Nigeria_Doubtful_Verifications/Nexus_Exports_Nigeria_Doubtful_Verifications.xlsx'
LOCAL_FILE_PATH = '/tmp/Nexus_Exports_Nigeria_Doubtful_Verifications.xlsx'

# Email configuration
SENDER = 'xxxx'
# Add the new recipients to the existing RECIPIENT list
RECIPIENT = 'xxxx, xxxx'

# Add CC recipients
CC = 'xxxx'

# Append current date to the file name
current_date = datetime.now().strftime('%Y-%m-%d')
SUBJECT = f'Nexus Exports Nigeria - Doubtful Verifications Report ({current_date})'
file_name = f'Nexus_Exports_Nigeria_Doubtful_Verifications_{current_date}.xlsx'

# Calculate insights
total_loaded = len(nexus_data)
total_fullyverified = sum(nexus_data['verified_cc'] == "FULLY VERIFIED")
total_doubtfulverified = sum(nexus_data['verified_cc'] == "DOUBTFUL VERIFIED")

# Construct email body with insights
BODY = f"""
Please find attached the Excel report of Nexus Exports Nigeria - Doubtful Verifications ({current_date}) which contains data from July 1st, 2024 to date. The file also contains the sample loaded to nexus for Nigeria today (Check sheet two)

Key Insights:
- Total Records: {total_loaded}
- Fully Verified Records: {total_fullyverified}
- Doubtful Verified Records: {total_doubtfulverified}

Best regards,
Business Intelligence Team
"""

# SMTP configuration
SMTP_SERVER = 'xxxx.office365.com'
SMTP_PORT = xxx  # Standard port for TLS
SMTP_USER = xxxx'
SMTP_PASSWORD = 'xxxx'  # Use the password provided

# Download the Excel file from S3
s3_client = boto3.client('s3')
s3_client.download_file(S3_BUCKET, S3_KEY, LOCAL_FILE_PATH)

# Create email
msg = MIMEMultipart()
msg['From'] = SENDER
msg['To'] = RECIPIENT
msg['Cc'] = CC
msg['Subject'] = SUBJECT

# Attach the body with the msg instance
msg.attach(MIMEText(BODY, 'plain'))

# Open the Excel file to be sent
with open(LOCAL_FILE_PATH, 'rb') as attachment:
    # Instance of MIMEBase and named as part
    part = MIMEBase('application', 'octet-stream')

    # To change the payload into encoded form
    part.set_payload(attachment.read())

    # Encode into base64
    encoders.encode_base64(part)

    # Add header with filename (with date)
    part.add_header(
        'Content-Disposition',
        f'attachment; filename={file_name}',
    )

    # Attach the instance 'part' to the instance 'msg'
    msg.attach(part)

# Send email
try:
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SENDER, RECIPIENT.split(',') + CC.split(','), msg.as_string())
        print("Email sent successfully!")
except smtplib.SMTPException as e:
    print(f"Error sending email: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

