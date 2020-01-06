import boto3
import json

REGION = "us-east-1"
FROM_ADDR = "xxx@co.jp"

def send_mail(from_addr, to_addr, subject, body):
    client = boto3.client('ses', region_name=REGION)

    response = client.send_email(
        Source=from_addr,
        Destination={
            'ToAddresses': [
                to_addr,
            ]
        },
        Message={
            'Subject': {
                'Data': subject,
            },
            'Body': {
                'Text': {
                    'Data': body,
                },
            }
        }
    )

    return response

def lambda_handler(event, context):
    """send_mail handler
    
    Args:
        event ([type]): [description]
        context ([type]): [description]
    
    Returns:
        json: response data of sending mail

    Requirement of input parameters
    "Result": {
        "subject": "subject",
        "to_addr": "xxx@co.jp",
        "body": "body",
        "status_code": 200
      }

    For error hundled
    "Error":{
        ~
    }
    "Result": {
        "subject": "subject",
        "to_addr": "xxx@co.jp",
        "body": "body",
        "status_code": 401
    }

    """
    from_addr = FROM_ADDR
    to_addr = str(event['Result']['to_addr'])
    subject = str(event['Result']['subject'])
    body = str(event['Result']['body'])
    if event['Result']['status_code'] != 200:
        body += json.dumps(event['Error'], indent=4)
    res = send_mail(from_addr, to_addr, subject, body)
    return res