from catalyst.constants import SQS_MODEL_QUEUE_NAME

import boto3


class ModelQueueAdapter:
    def __init__(self, region: str = "ap-northeast-1", verbose: bool = False):
        """
        Region must be a valid AWS region.
        """
        self.sqs = boto3.client("sqs", region_name=region)
        self.verbose = verbose

    def send_file(self, file_path: str, queue_url: str = SQS_MODEL_QUEUE_NAME):
        """
        Given a path to an ONNX file, sends the bytes to the Walkway Model Queue.
        """
        if not (file_path.endswith(".onnx")):
            raise Exception("Model Queue only accepts ONNX models.")
        with open(file_path, "r") as file:
            file_content = str(file.read())
        sthn = file_content
        response = self.sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=sthn,
        )
        if self.verbose:
            print(f"File sent to SQS with message ID: {response['MessageId']}")


if __name__ == "__main__":
    mqa = ModelQueueAdapter()
    mqa.send_file("sthn.txt", SQS_MODEL_QUEUE_NAME)
