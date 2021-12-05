"""
Created on 04 December 2021
Created by Stephen Cloete

6. Data Loading Tasks (if applying for a Data Engineering Position)
Select a subset of columns (including the H3 index column) from the sr_hex.csv 
or the anonymised file created in the task above, and write it to the write-only S3 bucket.

Be sure to name your output file something that is recognisable as your work,
 and unlikely to collide with the names of others.
"""
from ds_code_challenge_creds import access_key, secret_key, aws_region
from boto3.session import Session

import boto3
import datetime

s3 = boto3.resource('s3',
    	aws_access_key_id=access_key,
    	aws_secret_access_key= secret_key,
		region_name=aws_region
		)

# Output Data Link found under commit 713cec6 (https://github.com/cityofcapetown/ds_code_challenge/commit/713cec6b90fb07e807c1360145e4b0246ce0084f)
s3.Object('cct-ds-code-challenge-output-data', 'sr_hex.csv/anon_sr_hex_stephen_cloete.csv').put(Body=open('sr_hex.csv/anon_sr_hex_stephen_cloete.csv', 'rb'))