import pymongo
import json
import pandas as pd
import csv
import datetime  as dt
import awswrangler as wr
from pytz import timezone

#~ Time Zone
time_zone = timezone('Asia/Colombo')

def lambda_handler(event, context):
    read_CSV()

    return {
        'statusCode': 200,
        'body': json.dumps('Save Successful!')
    }
    
def read_CSV():
    #~ Function Log
    current_time = dt.datetime.now(time_zone).strftime("%Y-%m-%d %H:%M:%S.%f")   
    print('Started_reading_the_CSV_file_at_' + current_time)
    
    #~ Getting CSV file
    s3_bucket = 'mediation-anomaly-csv'
    file_path = f"s3://{s3_bucket}/MediationDataFile.csv"
    
    #~ Read CSV file
    df = wr.s3.read_csv(path=file_path)

    #~ Create the json document to insert 
    med_raw_data = json.loads(df.to_json(orient = 'records', date_format = 'iso'))

    insert_data(med_raw_data)
    
    #~ Function Log
    current_time = dt.datetime.now(time_zone).strftime("%Y-%m-%d %H:%M:%S.%f")   
    print('Inserted_CSV_data_to_the_med_trans_data_collection_at_' + current_time)

    
def insert_data(raw_data):
    client = pymongo.MongoClient('mongodb://databaselink')
    db = client.mediationanomaly
    col = db.med_trans_data
    
    #~ Insert data
    col.insert(raw_data)

    #~ Close the connection
    client.close()
