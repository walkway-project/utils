from catalyst.constants import S3_DATA_BUCKET_NAME

import boto3

import os

class S3StorageInterface:

    def __init__(self, datapath = ""):
        self.s3 = boto3.client('s3')
        self.datapath = datapath

    def download_bucket(self, bucket_name:str = S3_DATA_BUCKET_NAME):
        objects = self.s3.list_objects_v2(Bucket=bucket_name)['Contents']
        for obj in objects:
            key = obj["Key"]
            filepath = os.path.join(self.datapath, key)
            self.s3.download_file(bucket_name, filepath)


    def clear_bucket(self, bucket_name:str=S3_DATA_BUCKET_NAME):
        objects = self.s3.list_objects_v2(Bucket=bucket_name)['Contents']
        for obj in objects:
            key = obj["Key"]
            self.s3.delete_object(Bucket=bucket_name, Key=key)

