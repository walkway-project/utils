from catalyst.constants import SQS_MODEL_QUEUE_NAME

import boto3

class ModelQueueAdapter:

    def __init__(self, region:str = "ap-northeast-1"):
        self.sqs = boto3.client('sqs', region_name=region)

    def send_file(self, file_path, queue_url:str=SQS_MODEL_QUEUE_NAME):
        with open(file_path, 'r') as file:
            file_content = str(file.read())
        sthn = file_content
        print(sthn)
        response = self.sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=sthn,
        )

        print(f"File sent to SQS with message ID: {response['MessageId']}")


if __name__ == "__main__":
    mqa = ModelQueueAdapter()
    mqa.send_file("sthn.txt", SQS_MODEL_QUEUE_NAME)
