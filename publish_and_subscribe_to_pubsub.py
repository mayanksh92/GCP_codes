import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account

# Path to your service account key file
# key_path = "path/to/your/my_service_1.json"

# Authenticate using the service account key file
credentials = service_account.Credentials.from_service_account_file('C:\\Users\\Akanksha\\Downloads\\my_service_1.json')

# Project ID and Topic ID
project_id = "learning-gcp-424417"
topic_id = "python_topics"
subscription_id = "python_subscription"

# Create a Publisher client
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_id)

# Create a Subscriber client
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print("subscription path....", subscription_path)
# Function to publish a message to the Pub/Sub topic
def create_topic():
    topic = publisher.create_topic(request={"name": topic_path})
    print(f"Created topic: {topic.name}")

def publish_message(message_text):
    try:
        message_bytes = message_text.encode("utf-8")
        future = publisher.publish(topic_path, message_bytes)
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"An error occurred while publishing: {e}")

# Create subscription
def create_subscription():
    subscription = subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
    print(f"{subscription} subscription created..!!")

# Function to handle received messages
def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()
    print("checking message.ack.....",message.ack)
    print("checking message.data.....",message.data)

# Function to receive messages from the Pub/Sub subscription
def receive_messages():
    try:
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        print(f"Listening for messages on {subscription_path}..\n")
        print("checking the type of future....",type(streaming_pull_future))
        # Keep the main thread alive while waiting for messages
        with subscriber:
            try:
                streaming_pull_future.result()
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()
    except Exception as e:
        print(f"An error occurred while receiving messages: {e}")

if __name__ == "__main__":

    # Create a topic
    # create_topic()

    # Publish a test message
    # publish_message("Hello from python, Pub/Sub!")

    # create subscription
    # create_subscription()
    # Receive messages (this will keep running to listen for messages)
    receive_messages()
