import boto3

def create_and_subscribe_topic(email: str) -> str:
    sns = boto3.client("sns", region_name="us-east-1")

    topic = sns.create_topic(Name=''.join(c for c in email if c.isalnum()))
    topic_arn = topic["TopicArn"]

    subscription = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    return topic_arn


def publish_to_topic(topic_arn, subject, body):
    sns = boto3.client("sns", region_name="us-east-1")
    sns.publish(TopicArn=topic_arn, Subject=subject, Message=body)
