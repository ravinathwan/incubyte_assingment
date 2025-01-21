
from snowflake.snowpark import Session
import boto3
import io
import re

class s3_client:
    """
    A class to interact with AWS S3 buckets.

    Attributes:
        access_key_id (str): The AWS access key ID.
        secret_access_key (str): The AWS secret access key.
        bucket_name (str): The name of the S3 bucket to connect to.
        region (str) : By Default region will be 'eu-west-1' / Name of the region where S3 bucket lies
    """
    def __init__(self,bucket_name:str , access_key:str, secret_key:str, region = 'eu-west-1'):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.s3_client = self._create_s3_client()

    def _create_s3_client(self):
        return boto3.client('s3', aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key,region_name = self.region )

    def list_files(self,folder_prefix: str) -> list:   
        """
        Function to List all the Files inside the AWS S3 Folder
         Attributes:
            folder_prefix (str): 'data-lake/cyara-sit/VoiceTestResult/'

        """
        operation_parameters = {'Bucket': self.bucket_name,
                        'Prefix': folder_prefix}
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(**operation_parameters)
        #filtered_iterator = page_iterator.search(f"Contents[?contains(Key,`2024_03_`)|| contains(Key, `2024_04_`)]")
        #filtered_iterator = page_iterator.search("Contents[?Size > `500000`]")
        response =[]
        for key_data in page_iterator:
            response.extend(key_data['Contents'])
        return response


    def fetch_json(self, file_key:str):
        """
        Function to read the json files 
        Attributes:
            file_key (str): 'data-lake/github_json/issues/2024_03_31_1711917526756_0.jsonl'
        Example pd.read_json(s3.fetch_json('data-lake/github_json/issues/2024_03_31_1711917526756_0.jsonl'),lines=True)
        """
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
        return response['Body']
    
    def fetch_parquet(self, file_key:str):
        """
        Function to read the Parquet files 
        Attributes:
            file_key (str): 'data-lake/github_json/issues/2024_03_31_1711917526756_0.jsonl'
        Example pd.read_parquet(s3.fetch_parquet('data-lake/aha_parquet/goals/2024_02_26_1708906210762_0.parquet'))
        """
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
        body_content = response['Body'].read()
        buffer = io.BytesIO(body_content)
        return buffer

