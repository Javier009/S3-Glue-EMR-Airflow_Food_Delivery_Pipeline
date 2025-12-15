import boto3

# Create the session once (already done)
try:
    session = boto3.Session(profile_name="AdministratorAccess-978177281350", region_name="us-east-2")
    sqs_client = session.client('sqs')
    lambda_client = session.client('lambda')
except:
    sqs_client = boto3.client('sqs')
    lambda_client = boto3.client('lambda')

QUE_URL = 'https://sqs.us-east-2.amazonaws.com/978177281350/food-delivery-raw-data-que'
TARGET_LAMBDA_FUNCTION_NAME = 'data_consumer_sqs_to_S3'

def get_queue_number_of_messages(queue_url):
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            'ApproximateNumberOfMessages'
        ]
        )['Attributes']
    number_of_messages = response['ApproximateNumberOfMessages']
    print(response)
    return int(number_of_messages)

def lambda_number_of_invocations(messages_in_queue, max_batch_size_per_invocation=5000):
    invokations = 1 if messages_in_queue <= max_batch_size_per_invocation else \
        (messages_in_queue / max_batch_size_per_invocation) if messages_in_queue % max_batch_size_per_invocation == 0 else \
        round((messages_in_queue / max_batch_size_per_invocation)) + 1
    print(f"Messages in Queue: {messages_in_queue}, Lambda of Lambda Invokations Required: {invokations}")
    return invokations

def invoke_lambda_function(number_of_invokations):
    for _ in range(number_of_invokations):
        response = lambda_client.invoke(
            FunctionName=TARGET_LAMBDA_FUNCTION_NAME,
            InvocationType='Event'  # Asynchronous invocation

        )
        print(f"Invoked {TARGET_LAMBDA_FUNCTION_NAME}, Response Status Code: {response['StatusCode']}")

def orchestrator():
    try:
        messages_in_queue = get_queue_number_of_messages(QUE_URL)
        if messages_in_queue == 0:
            print("No messages in the queue to process.")
            return
        number_of_invokations = lambda_number_of_invocations(messages_in_queue)
        invoke_lambda_function(number_of_invokations)
    except Exception as e:
        print(f"Error in orchestrator: {str(e)}")

# if __name__ == "__main__":
#     orchestrator()