
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

class SnowflakeClient:
    """
    A class to interact with Snowflake.

    Attributes:
        user (str): Snowflake User ID
        password (str) : Snowflake Password
        account (str) : Snowflake account Number
        warehouse (str) : Virtual Warehouse e.g "TRANSFORM_WH"

    """

    def __init__(self, user, password, account, warehouse  ):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.session = None

    def create_session(self):
        connection_parameters = {
            "user": self.user,
            "password": self.password,
            "account": self.account,
            "warehouse": self.warehouse,
        }
        self.session = Session.builder.configs(connection_parameters).create()
        return self.session

    def __enter__(self):
        """
        This method is called when entering a context manager with the `with` statement.
        It creates a session and begins a transaction.
        Example
        With Conn:
            conn.execute
        
        """
        self.create_session()
        self.session.sql("begin;").collect()
        return self.session
    
    def __exit__(self, exc_type, exc_value, traceback):

        """
        This method is called when exiting a context manager with the `with` statement.
        It either commits or rolls back a transaction based on the presence of an exception.
        """

        if exc_type:
            self.session.sql("rollback;").collect()
        else:
            self.session.sql("commit;").collect()
            
        self.session.close()
        return self

    def execute_query(self, query: str , database: str  , schema: str) -> list:
        """
   Function to Execute Query in Snowflake. (DML - Insert/update/Delete/call Stored Procedure Statement )

    Attributes:
        query (str): The SQL query to be executed, e.g., any DML statement.
        database : Snowflake Database name where query will execute
        schema : schema within the specified database
    """
        
        try:
            self.session.sql(f"USE {database}.{schema}").collect()
            
            return self.session.sql(query).collect()
        except Exception as e:
            print(f"Error executing query: {e}")
            raise
           
    def write_to_snowflake(self, dataframe: str, table: str, database: str, schema: str ,chunk = 100000):
        """
   Function to Write DataFrame into Snowflake Tables.

    Attributes:
        dataframe (str): Pandas Dataframe which we need to insert in Snowflake Table
        table (str): Snowflake Table where Dataframe will be Written
        database (str) : Snowflake Database name where Table Exists
        schema (str): schema within the specified database
        chunk : By Default 100,000  chunks of rows will be inserted into Snowflake
    """
        try:
            self.session.write_pandas(dataframe, table_name=table,
                                    database=database, schema=schema ,chunk_size = chunk , index=False)
        except Exception as e:
            print(f"Error writing DataFrame to Snowflake: {e}")

    def table_schema(self,table_list: list, database: str ,schema: str) -> dict:
        """
   Function to Fetch the Snowflake Tables Columns & Data Types.

    Attributes:
        table_list (list) :- ['employees','timeoff'] List of Input Tables for which we need to fetch the Column name & Data Types
        database (str):- Snowflake database name where table exists
        schema (str) :- Snowflake schema where table exists 

    Output:
            dictonary with key as table name & value as column name & data type
            Useful for creation of Temp Tables        
    """


        query_template = """ SELECT COLUMN_NAME 
                                , Case when  CHARACTER_MAXIMUM_LENGTH is null then  DATA_TYPE
                                else CONCAT(DATA_TYPE ,'(', CHARACTER_MAXIMUM_LENGTH  , ')') end as DATA_TYPE 
                            FROM {database}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_NAME = '{table_name}' and TABLE_SCHEMA = '{schema}' ; """
        
        result_dict = {}

        for table_name in table_list:

            try:

                raw_data = self.execute_query(query = query_template.format( table_name=table_name ,database=database, schema = schema) ,database=database, schema=schema)
            
                result_dict[table_name] = {row.COLUMN_NAME: row.DATA_TYPE for row in raw_data}
            
            except Exception as e:
                print(f"Error executing query: {e}")

        return result_dict