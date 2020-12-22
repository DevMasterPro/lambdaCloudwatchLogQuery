import json
import boto3
import datetime
from datetime import datetime
import time

def lambda_handler(event, context):
    try:
        logGroupName = event['logGroupName']
        topicArn     = event['TopicArn']
        subject      = event['Subject']
        snsmessage ="The following lines contains errors:"
        notification = False
        flag = True
        token=''
        def default(o):
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()    
        
        def convertToMil(value):
            dt_obj = datetime.strptime(str(value),'%Y-%m-%d %H:%M:%S')
            result = int(dt_obj.timestamp())
            # result = int(dt_obj.timestamp() * 1000)
            return result
        
            
        midnight = datetime.now().strftime("%Y-%m-%d 00:00:00")
        getYesterdayMilsec = convertToMil(str(midnight))
        print (getYesterdayMilsec)
        
        
        currentdateTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        getCurrentMilsec = convertToMil(str(currentdateTime))
        print(getCurrentMilsec)
        
        
        client = boto3.client('logs')
        response = client.start_query(
            logGroupName= logGroupName,
            startTime=getYesterdayMilsec,
            endTime=getCurrentMilsec,
            queryString='filter @message like /ERROR/ and @message like /Document/'
        )
        
        data = json.dumps(response, indent=2, default=default)
        jsonResult = json.loads(data)
        print(jsonResult['queryId'])
        
        
        time.sleep(5)
        quaryResponse = client.get_query_results(queryId=jsonResult['queryId'])
        quaryResponsedata = json.dumps(quaryResponse, indent=2, default=default)
        quaryResponsejsonResult = json.loads(quaryResponsedata)
        print(quaryResponsejsonResult)
        
        
        print(str(len(quaryResponsejsonResult["results"])))
        
        listCount= len(quaryResponsejsonResult["results"])
        i = 0
        if listCount > 0:
            while i < listCount:
                for obj in quaryResponsejsonResult["results"][i]:
                    # print(obj["message"])
                    if 'ERROR' in obj["value"] and 'Document' in obj["value"]:
                        print("An Error found --> " + obj["value"])
                        snsmessage += "\n" + obj["value"]
                        notification = True
                        i = i + 1
    
        
            snsClient= boto3.client('sns')
            
            if notification == True:
                sns = snsClient.publish(
                    TopicArn=topicArn,
                    Message=snsmessage,
                    Subject=subject,
                )
        else:
            print("The List is empty.")
        
        return {
            'StatusCode': 200,
            'Message': 'Successfully executed the function '
        }
    
    except Exception as e:
        print("Something went wrong, please investigate")
        
        return {
            'StatusCode': 400,
            'Message': 'Something went wrong, Please Investigate. Error --> '+ str(e)
        }
        

