import boto3
import json
import random
from datetime import datetime, timedelta
import uuid

#S3 Config

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

sns = boto3.client('sns',
                   region_name='us-east-2',
                    aws_access_key_id=session.get_credentials().access_key,
                    aws_secret_access_key=session.get_credentials().secret_key,
                    aws_session_token=session.get_credentials().token
                   )

QUE_URL = 'https://sqs.us-east-2.amazonaws.com/978177281350/food-delivery-raw-data-que'
NOTIFICATION_TOPIC = 'arn:aws:sns:us-east-2:978177281350:orders_raw_data_recieved'

def generate_order_records(n_records):

    records = []
    
    cuisine_types = ['Italian', 'Chinese', 'Mexican', 'Indian', 'Japanese', 'American', 'Thai', 'French']
    order_statuses = ['Placed', 'Preparing', 'Out for Delivery', 'Delivered', 'Cancelled']
    payment_methods = ['Credit Card', 'Debit Card', 'Digital Wallet', 'Cash']
    delivery_modes = ['Bicycle', 'Motorized Scooter', 'Car', 'On Foot']
    traffic_conditions = ['Light', 'Moderate', 'Heavy']

    
    for _ in range(n_records):

        order_id = f"ORDR-{datetime.now().strftime('%Y%m%d')}-{str(uuid.uuid4())[:8].upper()}"
        customer_id = f"CUST-{random.randint(10000, 99999)}"
        restaurant_id = f"RSTR-{random.randint(100, 999)}"
        driver_id = f"DRVR-{random.randint(1000, 1999)}"

        order_placed_at = datetime.now() - timedelta(minutes=random.randint(10, 50))
        order_status = random.choice(order_statuses)
        delivery_address_zip = f"{random.randint(90000, 99999)}"
        restaurant_subzone = random.choice(['Downtown', 'Mid-Market', 'Uptown', 'Chinatown'])
        cuisine_type = random.choice(cuisine_types)

        subtotal = round(random.uniform(15.0, 100.0), 2)
        tax = round(subtotal * 0.085, 2)
        service_fee = round(random.uniform(1.0, 5.0), 2)
        delivery_fee = round(random.uniform(3.0, 7.0), 2)
        discount_amount = round(random.uniform(0.0, 10.0), 2)
        total_paid = round(subtotal + tax + service_fee + delivery_fee - discount_amount, 2)
        payment_method = random.choice(payment_methods)

        promised_time_minutes = random.randint(20, 60)
        prep_start_time = order_placed_at + timedelta(minutes=random.randint(5, 15))
        driver_pickup_time = prep_start_time + timedelta(minutes=random.randint(10, 20))
        actual_delivery_time = driver_pickup_time + timedelta(minutes=random.randint(10, 20))
        minutes=random.randint(5, 15)
        delivery_mode = random.choice(delivery_modes)
        distance_km = round(random.uniform(1.0, 10.0), 2)
        traffic_condition = random.choice(traffic_conditions)
        items_purchased = [
            {
                "item_id": f"MNU-{random.randint(100, 999)}",
                "item_name": random.choice(['Pizza', 'Burger', 'Sushi Roll', 'Pasta', 'Tacos']),
                "unit_price": round(random.uniform(5.0, 25.0), 2),
                "quantity": random.randint(1, 3),
                "customizations": random.choices(['Extra Cheese', 'No Onions', 'Spicy', 'Gluten-Free'], k=random.randint(0,2))
            }
            for _ in range(random.randint(1, 4))
            ]   

        feedback = {
            "food_rating": random.randint(1, 5),
            "driver_rating": random.randint(1, 5),
            "review_comment": random.choice(["Great service!", "Food was cold.", "Arrived late.", "Excellent quality!", "Will order again."]),
            "delivery_person_tip": round(random.uniform(0.0, 10.0), 2)
        }

        record = {
            'order_id': order_id,
            'customer_id': customer_id,
            'restaurant_id': restaurant_id,
            'driver_id': driver_id,
            'order_placed_at': order_placed_at.isoformat(),
            'order_status': order_status,
            'delivery_address_zip': delivery_address_zip,
            'restaurant_subzone': restaurant_subzone,
            'cuisine_type': cuisine_type,
            'pricing': {
                'subtotal': subtotal,
                'tax': tax, 
                'service_fee': service_fee,
                'delivery_fee': delivery_fee,
                'discount_amount': discount_amount,
                'total_paid': total_paid,
                'payment_method': payment_method
            },
            'logistics': {
                'promised_time_minutes': promised_time_minutes,
                'minutes': minutes,
                'prep_start_time': prep_start_time.isoformat(),
                'driver_pickup_time': driver_pickup_time.isoformat(),
                'actual_delivery_time': actual_delivery_time.isoformat(),
                'delivery_mode': delivery_mode,
                'distance_km': distance_km,
                'traffic_condition': traffic_condition
            },
            'items_purchased': items_purchased,
            'feedback': feedback
        }
    
        records.append(record)
    return records
    

def send_records_to_sqs(queue_url:str, records:list):
    for record in records:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(record)
        )
    return response


if __name__ == "__main__":
    
    number_of_records = 300
    # number_of_records = random.randint(50000, 60000)
    records = generate_order_records(number_of_records)
    print(len(records), "records generated.")
    response = send_records_to_sqs(QUE_URL, records)
    # Send notification to SNS topic
    sns.publish(
        TopicArn=NOTIFICATION_TOPIC,
        Message=f"{number_of_records-1} new order records have been sent to SQS.",
        Subject="New Order Records Notification"
    )
    print("Notification sent to SNS topic.")
    print(f"Sent {number_of_records-1} records to SQS with MessageId: {response['MessageId']}")
