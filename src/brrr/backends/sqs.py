from mypy_boto3_sqs import SQSClient

from ..queue import Queue, Message, QueueIsEmpty

class SqsQueue(Queue):
    def __init__(self, client: SQSClient, url: str):
        self.client = client
        self.url = url

    def put(self, message: str):
        self.client.send_message(
            QueueUrl=self.url,
            MessageBody=message
        )

    def get_message(self) -> Message:
        response = self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=3,
        )

        if "Messages" not in response:
            raise QueueIsEmpty

        return Message(response["Messages"][0]["Body"], response["Messages"][0]["ReceiptHandle"])

    def delete_message(self, receipt_handle: str):
        self.client.delete_message(
            QueueUrl=self.url,
            ReceiptHandle=receipt_handle
        )

    def set_message_timeout(self, receipt_handle, seconds):
        self.client.change_message_visibility(
            QueueUrl=self.url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=seconds
        )

