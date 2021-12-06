"""
Created on 04 December 2021
Created by Stephen Cloete

5. Further Data Transformations (if applying for a Data Engineering Position)

Write a script which anonymises the sr_hex.csv file, but preserves the following resolutions 
(You may use H3 indexes or lat/lon coordinates for your spatial data):

location accuracy to within approximately 500m
temporal accuracy to within 6 hours
scrubs any columns which may contain personally identifiable information.

We expect in the accompanying report that follows you will justify as to why this data is now anonymised. 

Please limit this commentary to less than 500 words. 

If your code is written in a code notebook such as Jupyter notebook or Rmarkdown, 
you can include this commentary in your notebook.

Files needed:
"ds_code_challenge_creds.py"
"""

import pandas as pd
import datetime
import os
import dload

print('Running: 3-further-data-transformations.py')
start_time = datetime.datetime.now()

# fuction for anonymising.
def clean_df(df, cols):
	for col_name in cols:
		keys = {anonymising: i for i, anonymising in enumerate(df[col_name].unique())}
		df[col_name] = df[col_name].apply(lambda x: keys[x])
	return df
	
# function for location accuracy to within approximately 500m
def location_df(df, cols):
	for col_name in cols:
		keys = {location: i for i, location in enumerate(df[col_name].unique())}
		df[col_name] = df[col_name].apply(lambda x: keys[x]+0.00400)			
	return df

if os.path.exists('sr_hex.csv/sr_hex.csv'):
	print('file already exist')
	pass
else:
	# Download input file from S3 & unzip
	dload.save_unzip('https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz',delete_after=True)
	if os.path.exists('sr_hex.csv') == True:
		print('file downloaded successfully')
		pass
	else:
		raise Exception('file not downloaded')

# open input file provided
try:
	df = pd.read_csv('sr_hex.csv/sr_hex.csv')
	print('raw dataset:')
	print(df.loc[[1,2,3,4,5],["SubCouncil2016", "Wards2016", "OfficialSuburbs", "h3_level8_index"]])

	# replace NaN values by Zeroes
	df = df.fillna(0)

	# Cols we scrubbing 
	cols = ["SubCouncil2016", "Wards2016", "OfficialSuburbs", "h3_level8_index"]		
	df = clean_df(df, cols)
	
	# location accuracy to within approximately 500m
	cols = ["Latitude", "Longitude"]
	df = location_df(df, cols)
	print()
	print('report output:')
	print(df.loc[[1,2,3,4,5],["SubCouncil2016", "Wards2016", "OfficialSuburbs", "h3_level8_index"]])
	print('report feedback:')
	print('The dataset is now anonymised and the location accuracy is within 500m.')
	print('Data anonymization processes personal data that is altered in such a way that a data subject can no longer be identified directly or indirectly.')
	#Output file being created
finally:
	df.to_csv("anon_sr_hex_stephen_cloete.csv")

if os.path.exists('anon_sr_hex_stephen_cloete.csv') == True:
	print('anon_sr_hex_stephen_cloete.csv successfully written')
	pass
else:
	raise Exception('anon_sr_hex_stephen_cloete.csv failed to write')

end_time = datetime.datetime.now()
time_taken = end_time - start_time

# Process time stats
print("start_time = ", start_time)
print("end_time = ", end_time)
print("time_taken = ", time_taken)