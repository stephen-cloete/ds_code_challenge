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
"""

import pandas as pd
import datetime
import os
import dload

start_time = datetime.datetime.now()

# fuction scrubbing data cols that needs to be anonymised.
def clean_df(df, cols):
	for col_name in cols:
		keys = {cats: i for i, cats in enumerate(df[col_name].unique())}
		df[col_name] = df[col_name].apply(lambda x: keys[x])
	return df
	
# function setting location accuracy to within approximately 500m
def location_df(df, l_cols):
	for col_name in l_cols:
		keys = {cats: i for i, cats in enumerate(df[col_name].unique())}
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

#open input file provided
try:
    df = pd.read_csv('sr_hex.csv/sr_hex.csv')
    # replace NaN values by Zeroes
    df = df.fillna(0)
    # Cols we scrubbing 
    print(df.head)
    cols = ["SubCouncil2016", "Wards2016", "OfficialSuburbs", "h3_level8_index"]		
    df = clean_df(df, cols)
    # location accuracy to within approximately 500m
    l_cols = ["Latitude", "Longitude"]
    df = location_df(df, l_cols)
    #Output file being created
	print('Report Output')
	print(df.head)
finally:
    df.to_csv("anon_sr_hex.csv")

if os.path.exists('anon_sr_hex.csv') == True:
    print('anon_sr_hex.csv successfully written')
    pass
else:
    raise Exception('anon_sr_hex.csv failed to write')


end_time = datetime.datetime.now()
time_taken = end_time - start_time

# Process time stats
print("start_time = ", start_time)
print("end_time = ", end_time)
print("time_taken = ", time_taken)