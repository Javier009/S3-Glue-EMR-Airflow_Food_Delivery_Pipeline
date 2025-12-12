import boto3
import json
from datetime import datetime
import time


QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/978177281350/food-delivery-raw-data-que'

# session = boto3.Session(
#     profile_name="AdministratorAccess-978177281350",
#     region_name="us-east-2"
# )

# sqs = boto3.client('sqs',
#                    region_name='us-east-2',
#                     aws_access_key_id=session.get_credentials().access_key,
#                     aws_secret_access_key=session.get_credentials().secret_key,
#                     aws_session_token=session.get_credentials().token
#                    )

# s3 = boto3.client(
#         's3',
#         region_name='us-east-2',
#         aws_access_key_id=session.get_credentials().access_key,
#         aws_secret_access_key=session.get_credentials().secret_key,
#         aws_session_token=session.get_credentials().token
#         )

sqs = boto3.client('sqs')
s3 = boto3.client('s3')
    
DESTINATION_BUCKET = 'food-delvery-raw-data-bucket'

def process_messages_from_sqs(queue_url=QUEUE_URL, max_total_pull=5000):

    batch = []
    max_total_pull = max_total_pull
    records_processed = 0

    while records_processed <= max_total_pull:

        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,  
                WaitTimeSeconds=5       
            )
            messages = response.get('Messages', [])
            if messages:
                for message in messages:

                    order = json.loads(message['Body'])
                    receipt_handle = message['ReceiptHandle']
                    batch.append(order)
                    
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle 
                        )
                    records_processed += 1
            else:
                print("No more messages in the queue.")
                break
        
        except Exception as e:
            print(f"Error receiving or processing messages: {str(e)}")
            break
    
    print(f"Total records pulled from SQS: {records_processed}")
    return batch

def data_split(batch:list, max_records_per_file:int=500):

    if len(batch) <= max_records_per_file:
        splitted_data = [batch]

    else:
        splitted_data = []
        intial_index = 0
        end_index = max_records_per_file

        while intial_index <= len(batch) - 1:
            slice_data = batch[intial_index:end_index]
            splitted_data.append(slice_data)
            intial_index = end_index
            end_index = end_index + max_records_per_file
    
    return splitted_data


def send_records_to_s3(batches:list):
    
    if batches:

        for batch in batches:

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

            time.sleep(5)  # To avoid overwhelming S3 with requests

def lambda_handler(event, context):
    try:
        orders_batch = process_messages_from_sqs(QUEUE_URL)
        splitted_data = data_split(orders_batch, max_records_per_file=500)
  
        send_records_to_s3(splitted_data)

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
    
# if __name__ == "__main__":
#     lambda_handler(None, None)