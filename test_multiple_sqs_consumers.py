import boto3
import json
import time
import threading

# AWS LocalStack Endpoint
LOCALSTACK_ENDPOINT = "http://localhost:4566"

# Initialize SNS & SQS Clients
sns_client = boto3.client(
    "sns", endpoint_url=LOCALSTACK_ENDPOINT, region_name="us-east-1"
)
sqs_client = boto3.client(
    "sqs", endpoint_url=LOCALSTACK_ENDPOINT, region_name="us-east-1"
)

# Configurations
SNS_TOPIC_NAME = "test-topic"
SQS_QUEUES = ["queue1", "queue2", "queue3"]


def setup_sns_sqs():
    """Setup SNS topic and multiple SQS queues, then subscribe queues to SNS."""
    # Create SNS Topic
    sns_topic = sns_client.create_topic(Name=SNS_TOPIC_NAME)
    topic_arn = sns_topic["TopicArn"]
    print(f"✅ Created SNS Topic: {topic_arn}")

    queue_urls = {}

    for queue_name in SQS_QUEUES:
        # Create SQS Queue
        sqs_queue = sqs_client.create_queue(QueueName=queue_name)
        queue_url = sqs_queue["QueueUrl"]

        # Get SQS Queue ARN
        queue_arn = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        # Subscribe SQS Queue to SNS
        sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # Allow SNS to publish to SQS (Policy update)
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "SQS:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                }
            ],
        }

        sqs_client.set_queue_attributes(
            QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)}
        )

        queue_urls[queue_name] = queue_url
        print(f"✅ Created and Subscribed SQS Queue: {queue_name}")

    return topic_arn, queue_urls


def publish_message(topic_arn, message):
    """Publish a message to SNS Topic."""
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps({"default": message}),
        MessageStructure="json",
    )
    print(f"📢 Published Message to SNS: {message}")


def consume_queue(queue_name, queue_url):
    """Worker to consume messages from an SQS queue."""
    print(f"👂 Worker started for {queue_name}...")

    while True:
        messages = sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5
        )

        if "Messages" in messages:
            for msg in messages["Messages"]:
                body = json.loads(msg["Body"])
                print(f"📥 {queue_name} received: {body}")

                # Delete the message after processing
                sqs_client.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"]
                )
        else:
            break  # Exit if no more messages


if __name__ == "__main__":
    # Setup SNS and SQS
    topic_arn, queue_urls = setup_sns_sqs()

    # Publish a single message to SNS
    publish_message(topic_arn, "Hello from SNS Fan-out!")

    # Allow SQS to process the message
    time.sleep(3)

    # Start multiple consumer threads
    threads = []
    for queue_name, queue_url in queue_urls.items():
        thread = threading.Thread(target=consume_queue, args=(queue_name, queue_url))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("✅ All workers finished processing.")
