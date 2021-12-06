"""
Created on 04 December 2021
Created by Stephen Cloete

1. Data Extraction (if applying for a Data Engineering Position)
Use the AWS S3 SELECT command to read in the H3 resolution 8 data from city-hex-polygons-8-10.geojson. Use the city-hex-polygons-8.geojson file to validate your work.

Please log the time taken to perform the operations described, and within reason, try to optimise latency and computational resources used.

Files needed:
"ds_code_challenge_creds.py"
"""
from ds_code_challenge_creds import access_key, secret_key, aws_region
from boto3.session import Session
from botocore.errorfactory import ClientError

import boto3
import datetime

print('Running: 1-data_extraction.py')

# Tracking Time taken for extraction to run
extraction_start_time = datetime.datetime.now()

# Compare new data set vs existing data set
source_data_list = []
compare_data_list = []

# create boto session
session = Session(
	aws_access_key_id=access_key,
	aws_secret_access_key=secret_key,
	region_name=aws_region
	)

# make connection
s3_session = session.client('s3')

# Check if files exists
try:
    s3_session.head_object(Bucket='cct-ds-code-challenge-input-data', Key='city-hex-polygons-8-10.geojson')
    s3_session.head_object(Bucket='cct-ds-code-challenge-input-data', Key='city-hex-polygons-8.geojson')
except ClientError:
    raise Exception('file/files not found')

# getting data from https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/city-hex-polygons-8-10.geojson
source_data = s3_session.select_object_content(
	Bucket = "cct-ds-code-challenge-input-data",
	Key = "city-hex-polygons-8-10.geojson",
	Expression = "SELECT d.* FROM  S3Object[*].features[*] d WHERE d.properties.resolution = 8",
	ExpressionType = "SQL",
	InputSerialization = {"JSON": {"Type": "DOCUMENT"}},
	OutputSerialization = {"JSON": {"RecordDelimiter": ", "}}
)

if source_data:
	print('city-hex-polygons-8-10.geojson Retrieval successful')
	pass
else:
	raise Exception('city-hex-polygons-8-10.geojson Retrieval unsuccessful')

# getting data from https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/city-hex-polygons-8.geojson
compare_data = s3_session.select_object_content(
				Bucket = "cct-ds-code-challenge-input-data",
				Key = "city-hex-polygons-8.geojson",
				Expression = "SELECT d.* FROM  S3Object[*].features[*] d",
				ExpressionType = "SQL",
				InputSerialization = {"JSON": {"Type": "DOCUMENT"}},
				OutputSerialization = {"JSON": {"RecordDelimiter": ", "}}
)

if compare_data:
	print('city-hex-polygons-8.geojson Retrieval successful')
	pass
else:
	raise Exception('city-hex-polygons-8.geojson Retrieval unsuccessful')

# Loop through objects then comparing it to make sure the data is the same
for data in source_data["Payload"]:
	if "Records" in data:
		source_data_list.append(data["Records"]["Payload"].decode())
		
for data in compare_data["Payload"]:
	if "Records" in data:
		compare_data_list.append(data["Records"]["Payload"].decode())

# Making sure the data from the two datasets matches
if source_data_list.sort() == compare_data_list.sort():
	print("match: city-hex-polygons-8-10.geojson dataset matches city-hex-polygons-8.geojson dataset")
elif source_data_list.sort() != compare_data_list.sort(): 
    print("mismatch: city-hex-polygons-8-10.geojson dataset does not match city-hex-polygons-8.geojson dataset")
    diff = source_data_list.sort() - compare_data_list.sort()
    print("difference:", diff)

extraction_end_time = datetime.datetime.now()
extraction_time_taken = extraction_end_time - extraction_start_time

# Process time stats
print("start_time    =  ", extraction_start_time)
print("end_time      =  ", extraction_end_time)
print("time_taken    =  ", extraction_time_taken)