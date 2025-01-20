from snowflake.snowpark import Session
import  os , json 
from datetime import datetime, timedelta
import copy
import time 
import asyncio

############################################################################################################
# Assumptions:
# 1. Using Snowflake as Destination
# 2. Using S3 as Source
# 3. Data Flow will be (Source [CDC/updated timestamp method] --> S3 --> Snowflake) -- Tools like Airbyte/Fivetran
######################################################################################################

# Function to Load JSON Files
def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Need to refresh Snowflake stages for gettingMeta data of newer files
def refresh_stage(session,stage):
    stage = stage[1:]
    session.sql(f"""ALTER STAGE  {stage} REFRESH""").collect();
    return None

# Get the Last Timestamp of 
def get_max_timestamp_loaded_files(session , history_table):
    try:
        lt_modified = session.sql(f"""Select case when max(AWS_LT_MODIFIED) is Null 
                                    then '1970-01-01 05:30:00'::TIMESTAMP_LTZ 
                                else DATEADD(minute , -18 , max(AWS_LT_MODIFIED)) end as max_lt_modified_timestamp
                                FROM {history_table}
                        WHERE   IS_PROCESSED = True  """).collect()
        
        return  lt_modified
    except Exception as e:
        print("Error in Function filter_condition_existing_files")
        raise e
    

# List all the S3 Files where Last Modified Date is greater than Loaded Files
def list_files(session,stage,lt_modified ):  
    """
    A Function to List S3 Files 
    """
    try:
        files = session.sql(f"""Select RELATIVE_PATH as file_name , LAST_MODIFIED, SIZE ,   
            split(REGEXP_SUBSTR(file_name ,'\\\d{{5,}}_\\\d+',1,1  ) ,'_')[0] :: Number as unix_timestamp ,
            split(REGEXP_SUBSTR(file_name ,'\\\d{{5,}}_\\\d+',1,1  ) ,'_')[1] :: Number as file_index
            from Directory('{stage}')
            where Last_Modified >= '{lt_modified}'::TIMESTAMP_LTZ
        ;""").collect()
        
        lt_modified_files = [file.as_dict() for file in files]
        return lt_modified_files

    except Exception as e:
        print("Error in List Files Function ")
        raise e   
     
# Asuming Table is already created in Snowflake , if needed we can generate DDL From JSON Schema
def table_creation(schema: dict , database: str) -> list:
        """
   Function to create Temporary Table Statement/query which will be executed in snowflake for Temporary Table Creation

    Attributes:
        table (dict) :- Input the Dictionary which is output of table_schema function
    Output:
           list :- create Temporary Table Statement/query       
    """
        schema = copy.deepcopy(schema)
        for i in schema:
            schema[i]['FILE_ROW_NUMBER'] = 'Number(8,0)'

        create_temp_tables_sql = {}

        for table_name, columns_dict in schema.items():
            # Generate CREATE TEMPORARY TABLE statement
            create_temp_table_sql = f"CREATE or replace TRANSIENT TABLE {database}TEMP_{table_name} ("
            create_temp_table_sql += ", ".join([f"{column} {data_type}" for column, data_type in columns_dict.items()])
            create_temp_table_sql += ");"
            #create_temp_tables_sql.append(create_temp_table_sql)

            create_temp_tables_sql[table_name] = create_temp_table_sql

        return create_temp_tables_sql

# Function to Load the Files into Snowflake Staged Table
def copy_into_statement(table:str, files:list, database:str,stage: str) -> str:
    # For Nested Json

    if len(files) == 1:
        files = f"('{files[0]}')"
    else:
        files = tuple(files)
    return """Copy into {database}TEMP_{table}  FROM  '{stage}/{table}/'
                        FILE_FORMAT = (TYPE = 'JSON', REPLACE_INVALID_CHARACTERS=TRUE   ) 
                        FILES = {files}
                         MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                        INCLUDE_METADATA = (File_Source = METADATA$FILENAME , FILE_ROW_NUMBER= METADATA$FILE_ROW_NUMBER)
                        ON_ERROR=ABORT_STATEMENT; """.format(table=table, files=files
                                                             ,database=database , stage = stage )
connection_parameters = {
            "user":  'xxxx.com',
            "password": 'sf_password',
            "account":  'sf_account',
            "warehouse":  'TRANSFORM_WH',
            "database" : "vacination_data",
            "schema" : 'sf_schema'
        }
print(f"Connection for {'sf_account'} ")
session = Session.builder.configs(connection_parameters).create()

# # # # # #  # # # #
# XS -     8       #
# S -      16      # 
# M -      32      #
# L -      64      #
# XL -     128     #  
# 2X-Large 256     #     
# # # # # # # # # # 

def main(session):
    script = 'dev'
    try:
        ETL_Batch_ID = int(time.time())
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parameter_path =  os.path.join(script_dir, 'pipeline_parameters.json')
        script_parameters = load_json_file(parameter_path)[script]
        table_schema = script_parameters['table_schema']

        # Load Script Parameters
        stage = script_parameters['stage'] 
        temp_schema = script_parameters['temp_schema']
        target_database = script_parameters['target_database']
        history_table = script_parameters['history_table']
        schema_path = os.path.join(script_dir, table_schema)
        table_config = load_json_file(schema_path)  

        #Step No 1 ;- Refresh Stage
        refresh_stage(session,stage)

        #step No 2 :- Get the Last Timestamp of Loaded Files
        lt_timestamp = get_max_timestamp_loaded_files(session , history_table)

        #step No 3 :- List the files  which are greater than Last Timestamp

        files_to_load = list_files(session,stage,lt_timestamp )

        # In Production we can use Async Pipeline & Chunk of Files to load in Database

        #step No 4 ;- Load files into Staging Table

        load_statement = copy_into_statement(table = 'stg_vacination', files = files_to_load
                                             , database = temp_schema,stage = stage)
        # In production I will also pass the BATCH ID for ETL Run so that we can track the data
        session.sql(load_statement).collect()

        # step No 5 ;- Get the List of Country in Staging Files

        country_list = session.sql(f"""Select distinct COUNTRY from {temp_schema}stg_vacination""").collect()

        # step No 6 :- Load the Data into target Table with country as Distinct country as filter
        
        for country in country_list:
            country = country.as_dict()['COUNTRY']
            session.sql(f"""Insert into {target_database}vaccination_data ({','.join(table_config.keys())} )
                            Select ({','.join(table_config.keys())} )
                              from {temp_schema}stg_vacination where COUNTRY = '{country}' """).collect()

    # For derived Columns 
    # I will create a view in Snowflake & asks end user to query the view as age , 
    # Last contacted days etc will be kept on varying


    # Validations can be added to check the data in target table
    # 1. Rows Loaded in stg table should be equal to all the rows loaded in target table
    # 2. In case of any Null in Country List Load that data in error_data with ETL_PIPE_ID & Filename , Integrate with Slack / Teams for 
    #     real time notification
    # 3. Create a Log Table for any error in Data Pipeline 

    except Exception as e:
        print("Error in Main Function ")
        raise e