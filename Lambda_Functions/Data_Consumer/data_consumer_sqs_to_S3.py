import boto3
import json
from datetime import datetime


QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/978177281350/food-delivery-raw-data-que'

session = boto3.Session(
    profile_name="AdministratorAccess-978177281350",
    region_name="us-east-2"
)

sqs = boto3.client('sqs',
                   region_name='us-east-2',
                    aws_access_key_id=session.get_credentials().access_key,
                    aws_secret_access_key=session.get_credentials().secret_key,
                    aws_session_token=session.get_credentials().token
                   )

s3 = boto3.client(
        's3',
        region_name='us-east-2',
        aws_access_key_id=session.get_credentials().access_key,
        aws_secret_access_key=session.get_credentials().secret_key,
        aws_session_token=session.get_credentials().token
        )
    
DESTINATION_BUCKET = 'food-delvery-raw-data-bucket'

def process_messages_from_sqs(queue_url):
    batch = []
    batch_size = 100
    records_processed = 0

    while records_processed < batch_size:

        try:
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,  
                WaitTimeSeconds=5       
            )
            messages = response.get('Messages', [])
            for message in messages:

                order = json.loads(message['Body'])
                receipt_handle = message['ReceiptHandle']
                batch.append(order)
                
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle 
                    )
                records_processed += 1
        
        except Exception as e:
            print(f"Error receiving or processing messages: {str(e)}")
            break
    return batch

def send_records_to_sqs(batch:list):
    
    if batch:

        now = datetime.now()
        s3_key =  (
        f"year={now.year}/"
        f"month={now.month:02}/"
        f"day={now.day:02}/"
        f"orders_data_{now.strftime('%Y-%m-%d_%H_%M_%S')}.json")

        json_data = json.dumps(batch, indent=1)

        s3.put_object(
            Bucket=DESTINATION_BUCKET,
            Key=s3_key,
            Body=json_data
        )

        print(f"Uploaded batch of {len(batch)} orders to S3://{DESTINATION_BUCKET}/{s3_key}")

def lambda_handler(event, context):
    try:
        orders_batch = process_messages_from_sqs(QUEUE_URL)
        send_records_to_sqs(orders_batch)

        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed and stored {len(orders_batch)} orders.")
        }
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps("Error processing orders.")
        }
    
lambda_handler(None, None)