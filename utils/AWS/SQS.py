import boto3

sqs = boto3.client('sqs')

def send_file_to_sqs(file_path, queue_url):
    with open(file_path, 'rb') as file:
        file_content = file.read()

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=file_content
    )

    print(f"File sent to SQS with message ID: {response['MessageId']}")


if __name__ == "__main__":
    send_file_to_sqs()
