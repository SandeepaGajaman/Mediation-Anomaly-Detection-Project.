import pymongo
import json
import pandas as pd
import datetime as dt
import fbprophet
from pytz import timezone
from fbprophet import Prophet
from holidays import WEEKEND, HolidayBase

#Time Zone
time_zone = timezone('Asia/Colombo')

def lambda_handler(event, context):
    stream_forcast()

    return {
        'statusCode': 200,
        'body': json.dumps('!! Code Executed Successfully !!')
    }

def stream_forcast():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.mediationanomaly
    
    #Function Log
    current_time = dt.datetime.now(time_zone).strftime("%Y-%m-%d %H:%M:%S.%f")   
    print('Started_stream_forcast_at_' + current_time)
    
    #Streams
    stream_arr = ['UPLA_AP1','UPLA_AP2','KALA_AP1','KALA_AP2','PILI_AP1','PILI_AP2','MGCF_KALA','MGCF_PILI','HGMSC3','HGMSC4','IMS_MALBE','IMS_KALA']
    
    for x in stream_arr:
        ##Specify the collection to be used
        col = db.med_trans_data

        # Query Cargills transaction data data
        cursor = col.find({"Stream": x}).sort("ds", 1 )

        # Query data into dataframe
        df = pd.DataFrame(list(cursor))
        
        #DateTimeIndex
        df['ds'] = pd.DatetimeIndex(df['ds'])
        
        ##Prophet##

        #Create model
        m = Prophet(interval_width=0.95, daily_seasonality=50, weekly_seasonality=False, yearly_seasonality=False)

        #Fit model
        m.fit(df)
        
        #Future timeframe fore forecast
        future = m.make_future_dataframe(periods=96, freq='15MIN', include_history=False)
        forecast = m.predict(future)

        #Insert the App ID into dataframe 
        forecast['Stream'] = x
    
        #Get the forecasted result
        forecast = forecast[['Stream','ds','yhat']]

        #Create the json document to insert 
        forecast = json.loads(forecast.to_json(orient = 'records', date_format = 'iso'))
    
        #Insert the document to DB
        insert_forecated_data(forecast)
        
        #Function Log
        current_time = dt.datetime.now(time_zone).strftime("%Y-%m-%d %H:%M:%S.%f")   
        print('Ended_stream_forcast_at_' + current_time)
        
        #Close the connection
        client.close()

def insert_forecated_data(data):
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.mediationanomaly
    
    ##Specify the collection to be used
    col = db.med_forecasted_data 

    #Insert data
    col.insert(data)

    #Close the connection
    client.close()
