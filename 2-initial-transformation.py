"""
Created on 04 December 2021
Created by Stephen Cloete

2. Initial Data Transformation
Join the file city-hex-polygons-8.geojson to the service request dataset, such that each service request is assigned to a single H3 hexagon. 
Use the sr_hex.csv file to validate your work.

For any requests where the Latitude and Longitude fields are empty, set the index value to 0.

Include logging that lets the executor know how many of the records failed to join, and include a join error threshold above which the script will error out. 
Please also log the time taken to perform the operations described, and within reason, try to optimise latency and computational resources used.

Files needed:
"ds_code_challenge_creds.py"
"""

from ds_code_challenge_creds import access_key, secret_key, aws_region
from botocore.errorfactory import ClientError
from boto3.session import Session
from io import StringIO
from csv import reader

import dload
import boto3
import pandas as pd
import os
import itertools
import csv
import datetime

print('Running: 2-intial-transformation.py')

# Tracking Time taken to run
start_time = datetime.datetime.now()

if os.path.exists('sr.csv/sr.csv'):
	print('file already exist')
	pass
else:
	# Download input file from S3 & unzip
	dload.save_unzip('https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz',delete_after=True)
	if os.path.exists('sr.csv/sr.csv') == True:
		print('file downloaded successfully')
		pass
	else:
		raise Exception('file not downloaded')

# create boto session
session = Session(
	aws_access_key_id=access_key,
	aws_secret_access_key=secret_key,
	region_name=aws_region
	)

# make connection to s3
s3_session = session.client('s3')

# Check if files exists in S3
try:
    s3_session.head_object(Bucket='cct-ds-code-challenge-input-data', Key='city-hex-polygons-8.geojson')
except ClientError:
    raise Exception('file not found')

# getting data from https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/city-hex-polygons-8.geojson
source_data = s3_session.select_object_content(
	Bucket = "cct-ds-code-challenge-input-data",
	Key = "city-hex-polygons-8.geojson",
	Expression = "SELECT d.properties FROM  S3Object[*].features[*] d",
	ExpressionType = "SQL",
	InputSerialization = {"JSON": {"Type": "DOCUMENT"}},
	OutputSerialization = {"JSON":{"RecordDelimiter": ", "}}
)

if source_data:
	print('city-hex-polygons-8.geojson Retrieval successful')
	pass
else:
	raise Exception('city-hex-polygons-8.geojson Retrieval failed')

sr_dataset = []
# open downloaded input file provided
with open('sr.csv/sr.csv', 'r') as read_obj:
    # pass the file object to reader() to get the reader object
	csv_reader = reader(read_obj)
	# Iterate over each row in the csv using reader object
	for row in csv_reader:
    # Pass reader object to list() to get a list of lists
		sr_dataset.append(row)

if sr_dataset:
	print('sr.csv Retrieval successful')
	pass
else:
	raise Exception('sr.csv not found')

records = []
s3_dataset = []

for event in source_data["Payload"]:
	if "Records" in event:
		records.append(event["Records"]["Payload"])
		
#  store dataset as a CSV format
file_str = ''.join(req.decode('utf-8') for req in records)
    
#  read CSV to dataframe
df = pd.read_csv(StringIO(file_str))

for index, row in df.iterrows():
	placeholder_list = []
	# h3_level8_index
	placeholder_list.append(row[0].split(":")[2].strip('"'))
	# db_latitude
	placeholder_list.append(row[1].split(":")[1])
	# db_longitude
	placeholder_list.append(row[2].split(":")[1].split("}")[0])
	s3_dataset.append(placeholder_list)

# open output file
with open('service_request.csv', 'w', encoding='UTF8', newline='') as f:
		writer = csv.writer(f)
		header = ['', 'NotificationNumber', 'NotificationType', 'CreationDate', 'CompletionDate', 'Duration', 'CodeGroup', 'Code', 'Open', 'Latitude', 'Longitude', 'SubCouncil2016', 'Wards2016', 'OfficialSuburbs', 'directorate', 'department', 'ModificationTimestamp', 'CompletionTimestamp', 'CreationTimestamp', 'h3_level8_index']

		# write the header to output file
		print('writing headers to output file service_request.csv')
		writer.writerow(header)
	
		# Check for requests where the Lat and Long records are empty, set index value to 0.
		for row1 in sr_dataset:
			if row1[10] == 'nan':
				existing_row = row1
				existing_row.append(0)
				writer.writerow(existing_row)
			for row2 in s3_dataset:
				if row1[10] == row2[2] and row1[9] == row2[1]:
					updated_row = row1.append(row2[0])
					writer.writerow(updated_row)
	
print('written dataset to output file service_request.csv')

end_time = datetime.datetime.now()
time_taken = end_time - start_time

# Process time stats
print('file available = service_request.csv')
print("start_time = ", start_time)
print("end_time = ", end_time)
print("durtation = ", time_taken)