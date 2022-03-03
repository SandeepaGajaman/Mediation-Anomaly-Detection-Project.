import pymongo
import matplotlib
import matplotlib.pyplot as plt
import json
import io
import re
import boto3
import pandas as pd
import datetime  as dt
import array as arr
from datetime import date, datetime, timedelta
from pytz import timezone

def lambda_handler(event, context):
    
    s3client = boto3.client("s3")
    s3 = boto3.resource('s3')
    client = pymongo.MongoClient('mongodb://databaselink')
    db = client.mediationanomaly
        
    awsakid = 'AWSAKID'
    awssecakid = 'AWSSECAKID'
    topicArn = 'arn:aws:sns:ap-southeast-1:emailtopic'
    snsClient = boto3.client('sns', aws_access_key_id=awsakid, aws_secret_access_key=awssecakid, region_name='ap-southeast-1')
    appId = "Test Subject"
    
    #~ Get current date
    today = str(date.today())
    dateformate = datetime.strptime(today, "%Y-%m-%d")
    modified_date = dateformate - timedelta(days=1)
    modifieddateformat = datetime.strftime(modified_date, "%Y-%m-%d")
    print(modifieddateformat)
    
    #~ Streams - Add new stream names to the below array
    stream_arr = ['UPLA_AP1','UPLA_AP2','KALA_AP1','KALA_AP2','PILI_AP1','PILI_AP2','MGCF_KALA','MGCF_PILI','HGMSC3','HGMSC4','IMS_MALBE','IMS_KALA']
    
    #~ Get forecasted dataset related to the current date 
    for stream in stream_arr:
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
                        "Stream" : "$Stream",
                        "ds" : "$ds",
                        "yhat" : "$yhat"
                    }
            }])
        
        client.close()
        
        #~ Append forecasted dataset related to the current date to an array
        forecasted_json_arr = []
        for i in forecasted_result:
            i = re.sub(':00.000Z', '', str(i))
            json_string = json.dumps(i)
            json_string = re.sub('"', '', json_string)
            json_string = re.sub("'", '"', json_string)
            forecasted_json_arr.append(json_string)
        
        #~ Create a JSON file for each forecasted stream    
        forecasted_json_arr = str(forecasted_json_arr)
        forecasted_json_arr = forecasted_json_arr.replace("'", "")
        s3client.put_object(Bucket = "mediation-anomaly-csv", Key = stream + "_Forecasted_Data.json", Body = str(forecasted_json_arr))
    
    #~ Get actual dataset related to the current date    
    for stream in stream_arr:
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
                        "Stream" : "$Stream",
                        "ds" : "$ds",
                        "yhat" : "$y"
                    }
            }])
        
        client.close()
        
        #~ Append actual dataset related to the current date to an array
        actual_json_arr = []
        for i in actual_result:
            i = re.sub(':00.000Z', '', str(i))
            json_string = json.dumps(i)
            json_string = re.sub('"', '', json_string)
            json_string = re.sub("'", '"', json_string)
            actual_json_arr.append(json_string)
        
        #~ Create a JSON file for each actual stream      
        actual_json_arr = str(actual_json_arr)
        actual_json_arr = actual_json_arr.replace("'", "")
        s3client.put_object(Bucket = "mediation-anomaly-csv", Key = stream + "_Actual_Data.json", Body = str(actual_json_arr))
        
        #~ Get Forecasted Data Frame
        content_object = s3.Object('mediation-anomaly-csv', stream + '_Forecasted_Data.json')
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        forecasted_df = pd.DataFrame(json_content)
        
        #~ Get Actual Data Frame
        content_object = s3.Object('mediation-anomaly-csv', stream + '_Actual_Data.json')
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        actual_df = pd.DataFrame(json_content)
        
        #~ Plot the Result
        ax = forecasted_df.plot(x='ds', y = 'yhat', kind = 'line', label='Forecasted Data', color='red', figsize=(10, 5))
        actual_df.plot(x='ds', y = 'yhat', kind = 'line', label='Actual Data', color='green', ax=ax)
        plt.gcf().autofmt_xdate()
        plt.title(stream)
        plt.grid()
        plt.xlabel("Date & Time")
        plt.ylabel("Request Count")

        #~ Save as an Image
        img_data = io.BytesIO()
        plt.savefig(img_data, format='png')
        img_data.seek(0)
        imagebucket = s3.Bucket("mediation-anomaly-csv")
        imagebucket.put_object(Body=img_data, ContentType='image/png', Key= stream + "-img.png")

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
