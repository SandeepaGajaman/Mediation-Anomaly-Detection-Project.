import json
import pymongo
import boto3
import datetime as dt
import pandas as pd
from datetime import date, datetime, timedelta
from pytz import timezone

#Time Zone
time_zone = timezone('Asia/Colombo')

s3client = boto3.client("s3")
s3 = boto3.resource('s3')
awsakid = 'AWSAKID'
awssecakid = 'AWSSECAKID'
topicArn = 'arn:aws:sns:ap-southeast-1:emailtopic'
snsClient = boto3.client('sns', aws_access_key_id=awsakid, aws_secret_access_key=awssecakid, region_name='ap-southeast-1')

def lambda_handler(event, context):
    
    detect_anomaly()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def detect_anomaly():
    ##Create a MongoDB client
    client = pymongo.MongoClient('mongodb://databaselink')
    
    ##Specify the database to be used
    db = client.mediationanomaly
    
    ##Get current date
    today = str(date.today())
    dateformate = datetime.strptime(today, "%Y-%m-%d")
    modified_date = dateformate - timedelta(days=1)
    modifieddateformat = datetime.strftime(modified_date, "%Y-%m-%d")
    
    ##Streams
    stream_arr = ['UPLA_AP1','UPLA_AP2','KALA_AP1','KALA_AP2','PILI_AP1','PILI_AP2','MGCF_KALA','MGCF_PILI','HGMSC3','HGMSC4','IMS_MALBE','IMS_KALA']
    
    ##Empty Arrays
    no_data_found = []
    anomaly50_arr = []
    anomaly20_arr = []

    for stream in stream_arr:
        ##Specify the collection to be used
        col = db.med_trans_data
        
        actual_result = col.aggregate([
            {
                "$match" :
                    { "ds" :
                        { "$regex" : modifieddateformat}
                    }
            },
            {
                "$match" :
                    { "Stream" :
                        { "$regex" : stream}
                    }
            },
            {
                "$project" :
                    {
                        "_id" : 0,
                        "yhat" : "$y"
                    }
            }])
        
        actual_result_df = pd.DataFrame(list(actual_result))
        actual_result_df = actual_result_df.rename(columns={'yhat': ''})
        actual_result_df[''] = actual_result_df[''].astype(int)
        actual_result_count = actual_result_df['']

        ##Specify the collection to be used
        col = db.med_forecasted_data
        
        forecasted_result = col.aggregate([
            {
                "$match" :
                    { "ds" :
                        { "$regex" : modifieddateformat}
                    }
            },
            {
                "$match" :
                    { "Stream" :
                        { "$regex" : stream}
                    }
            },
            {
                "$project" :
                    {
                        "_id" : 0,
                        "ds" : "$ds",
                        "yhat" : "$yhat"
                    }
            }])

        forecasted_result_df = pd.DataFrame(list(forecasted_result))
        
        forecasted_result_df['ds'] = forecasted_result_df['ds'].apply(str)
        forecasted_result_date = forecasted_result_df['ds']
        
        forecasted_result_df = forecasted_result_df.rename(columns={'yhat': ''})
        forecasted_result_df[''] = forecasted_result_df[''].astype(int)
        forecasted_result_count = forecasted_result_df['']
        
        for forecasted_date, actual_count, forecasted_count in zip(forecasted_result_date, actual_result_count, forecasted_result_count):
            
            forecasted_date = forecasted_date.replace('T', ' at ')
            forecasted_date = forecasted_date.replace(':00.000Z', '')
                
            if actual_count == 0 or forecasted_count == 0:
                no_data = 'No data found in ' + stream + ' at ' + forecasted_date + '.'
                no_data_found.append(no_data)
            else:
                ##Percentage Calculation
                quotient = actual_count / forecasted_count
                actual_percentage = quotient * 100
                actual_percentage = actual_percentage
                anomaly = 100 - actual_percentage
                anomaly = int(anomaly)
    
                if stream == 'MGCF_KALA' or stream == 'MGCF_PILI' or stream == 'HGMSC3' or stream == 'HGMSC4' or stream == 'IMS_KALA' or stream == 'IMS_MALBE':
                    if anomaly > 50 or anomaly < -50:
                        anomaly = str(anomaly)
                        notification = '* Stream ' + stream + ' - ' + forecasted_date + ' = ' + anomaly + '%'
                        anomaly50_arr.append(notification)
                else:
                    if anomaly > 20 or anomaly < -20:
                        anomaly = str(anomaly)
                        notification = '* Stream ' + stream + ' - ' + forecasted_date + ' = ' + anomaly + '%'
                        anomaly20_arr.append(notification)
                    
    #Close the connection
    client.close()
    
    ##No data found alert
    no_data_alert = '\n'
    no_data_alert = no_data_alert.join(no_data_found)
    response = snsClient.publish(TopicArn=topicArn, 
    Message= 'Hi Team,\nNo data found in below mentioned streams.\n\n' + no_data_alert + '\n\nBest Regards,\nCRM R&D Team', 
    Subject= 'Mediation No Data Found Alert (' + modifieddateformat + ')')
    
    ##Anomaly alert
    anomaly_arr = anomaly50_arr + anomaly20_arr
    alert_message = '\n'
    alert_message = alert_message.join(anomaly_arr)
    response = snsClient.publish(TopicArn=topicArn, 
    Message= 'Hi Team,\nDetected anomalies in below mentioned streams.\n\n' + alert_message + '\n\nBest Regards,\nCRM R&D Team', 
    Subject= 'Mediation Anomaly Alert (' + modifieddateformat + ')')
