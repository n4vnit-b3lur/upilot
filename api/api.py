from sqlite3 import dbapi2
import string
from fastapi import FastAPI, File, UploadFile, Request 
from fastapi.responses import FileResponse
from pydantic import BaseModel
import yaml

description = """
API for Configuration, Staging,
Extraction, Transformation and Loading.

## Configuration API

* Get all configurations of database servers
* Create configuration for database server
* Read configuration for database server
* Update configuration for database server
* Delete configuration for database server

## Staging API

* Get buckets in staging area
* Create staging S3 bucket
* Retrieves contents of s3 bucket 
* Delete s3 bucket
* Create folder in s3 bucket
* Retrieve all folders in an s3 bucket
* Upload file to a folder in s3 bucket
* Download file from a folder in s3 bucket

## Extraction

* API for data **extraction** from various sources:

Structured Data Sources:
* Databases (all tables)
* Table (single table from a database)
* CSV file
* Excel file

Unstructured Data Sources:
* Word document
* PDF document
* images
* videos

## Transformation

API to perform various transformations on dataframes that are the output of
extraction from structured data sources

* Transformation of data in Database tables (in staging)
* Transformation of dataframes in S3 buckets in staging

## Load

API to manage data warehouse and load data into the data warehouse:

* List all DBs in warehouse
* Create DB in warehouse
* Read DB in warehouse
* Delete DB in warehouse
* List all tables in DB in warehouse
* Create table in DB in warehouse
* Delete table in DB in warehouse
* load table in DB in warehouse

## Query 

* Query Datawarehouse for reporting
"""


api = FastAPI(title="API",
    description=description,
    version="0.0.1")
    #,
    # openapi_tags=tags_metadata)


@api.get("/")
def read_root():
    return {"result": False, "messages": "select an API path", "errors": ""}


# Get all configurations of database servers
@api.get("/v1/config/database/servers")
def get_config_db_servers():
    '''
    Retrieves names of all database servers from the ETL configuration file.

        inputs: 
            None
        output: 
            server-names configured in config file.
    '''
    # TBD
    return {'result': '["server1", "server2"]'}


# Create configuration for database server
class Server(BaseModel):
    server_name: str
    host: str
    port: int
    user: str
    password: str

@api.post('/v1/config/database/servers')
def create_config_db_server(server: Server):
    ''' 
    Creates configuration information for a server in the ETL configuration file.
    Add a new entry to db_servers.config file with server_name as key and 
    host, port, user, and password as the values

        inputs: 
            server_name, host, port, user, password
                e.g. "user='scott', password='password', host='127.0.0.1', 
                server_name='my_server'"
        output: 
            Configuration contents for all servers '''
    # TBD
    return {'result': f'created server {server.server_name} successfully'}


# Read configuration for database server
@api.get('/v1/config/database/servers/{server_name}')
def get_config_db_server(server_name: str):
    ''' 
    Return the details (host, port and user (no pwd) of a db server 
    
        inputs: 
            None
        output: 
            Configuration contents for the server including host, port, user
    '''
    # TBD
    return {
        f'server_name': {
            "host": "host1.mycompany.com",
            "port": 9088,
            "user": "db_admin"
        }
    }


# Update configuration for database server
@api.put('/v1/config/database/servers/{server_name}')
def udate_config_database_server(server: Server):
    ''' 
    Updates config entry for database <server name> to reflect
        new values in the in the post body 
        
        inputs: 
            host, port, user, password
        output: 
            Status message indicating updation
    '''
    #TBD
    return {'result': f'server {server.server_name} updated successfully'}


# Delete config for database server 
@api.delete('/v1/config/database/servers/{server_name}')
def delete_db_server(server_name: str):
    ''' 
    Deletes the server config for a specific server
    
        inputs: 
            None
        output: 
            Status message indicating status of deletion
    '''
    # TBD
    return {'result': f'server {server_name} deleted successfully'}


# Get buckets in staging area
@api.get('/v1/staging/s3/buckets')
def get_buckets():
    ''' 
    Get list of all S3 staging buckets 
    
        inputs: 
            None
        output: 
            list of all S3 buckets in staging area
    '''
    # TBD
    return {'result': '["bucket1", "bucket2"]'}


# Create staging S3 bucket
class Bucket(BaseModel):
    bucket_name: str
    region: str

@api.post('/v1/staging/s3/buckets')
def create_bucket(bucket: Bucket):
    ''' 
    Creates an s3 bucket 
    
        inputs: 
            bucket name, region
        output: 
            Status message of bucket creation
    '''
    # TBD
    return {'result': f'created bucket {bucket.bucket_name} successfully'}


# Read s3 bucket contents
@api.get('/v1/staging/s3/buckets/{bucket_name}')
def get_bucket(bucket_name: str, region: str):
    ''' 
    Retrieves contents of s3 bucket 
    
        inputs: 
            None
        output: 
            Contents of specified S3 bucket
    '''
    # TBD
    return {'result': f'<contents of bucket {bucket_name}>'}


# Delete s3 bucket
@api.delete('/v1/staging/s3/buckets/{bucket_name}')
def delete_bucket(bucket_name: str, region: str):
    ''' 
    Deletes the specified S3 bucket 
    
        inputs: 
            None
        output: 
            Status message of bucket deletion
    '''
    # TBD
    return {'result': f'bucket {bucket_name} in region {region} deleted successfully'}


# Create folder in s3 bucket
class Folder(BaseModel):
    folder_name: str

@api.post('/v1/staging/s3/buckets/{bucket_name}/folders')
def create_folder(bucket_name: str, folder: Folder):
    ''' 
    Creates a folder in the specified S3 bucket 
    
        inputs: 
            folder name
        output: 
            Status message of folder creation
    '''
    # TBD
    return {'result': f'folder {folder.folder_name} created in bucket {bucket_name} successfully.'}


# Retrieve all folders in an s3 bucket 
@api.get('/v1/staging/s3/buckets/{bucket_name}/folders')
def get_folders(bucket_name: str):
    '''
    Retrieves a list of all folders in an S3 bucket

        inputs: 
            none
        output:
            list of all folders
    '''

    # TBD
    return {'result': '["folder1", "folder2"]'}


# Upload file to a folder in s3 bucket 
@api.post('/v1/staging/s3/buckets/{bucket_name}/folders/{folder}')
async def create_upload_file(bucket_name: str, folder: str, file: UploadFile):
    '''
    Uploads a file to the S3 bucket and folder specified

    inputs: the file to upload
    output: status message of file upload
    '''
    return {'result': f'file {file.filename} uploaded'}


# Download file from a folder in s3 bucket
@api.get('/v1/staging/s3/buckets/{bucket_name}/folders/{folder}/objects/{object_name}, response_class=FileResponse')
async def get_download_file(
    bucket_name: str, 
    folder: str,
    object_name: str, 
    file_name: str):
    '''
    Downloads a file from S3 bucket to local machine

    inputs: filename (object) in the S3 bucket
    ouput: the file 
    '''
    # TBD
    return {'result': f'file {file_name} downloaded successfully'}  


# # CREATE SPECIFIC DATABASE
# class Database(BaseModel):
#     name: str

# @api.post('/v1/extract/db/servers/{alias}/databases/')
# async def create_database(database: Database):
#     ''' create database in server alias '''
#     # TBD
#     return {'result': f'created database {database.name} successfully'}


##### EXTRACTION API #####

# Extract all tables from specific database
@api.get('/v1/extract/db/servers/{server_name}/databases/{database}/tables')
def extract_database(server_name: str, database: str):
    '''
    Extracts all tables from a database into dataframes.
        
        inputs: 
            db_params: string containing details of DB, 
                eg. "user='scott', password='password', host='127.0.0.1', database='employees'"
        output: 
            True if all tables were extracted to dataframes and stored in S3, False otherwise 
    '''
    # TBD
    return {'result': f'extracted database {database} successfully'}


# Extract table from specific database
@api.get('/v1/extract/db/servers/{server_name}/databases/{database}/tables/{table_name}')
def extract_table(server_name: str, database: str, table_name: str):
    ''' 
    Extracts a table to a dataframe.
        
        inputs: 
            db_params: string containing details of DB, 
                eg. "user='scott', password='password', host='127.0.0.1', database='employees'"
            table_name: string - name of the table to extract
                eg. "emails"
        output: 
            True if the tables was extracted to dataframe and stored in S3, False otherwise
    '''
    # TBD
    return {'result': f'extracted table {table_name} successfully'}


@api.post("/v1/extract/csv")
async def extract_csv(file: UploadFile):
    '''
    Extracts a csv to a dataframe.
        
        inputs: 
            file: csv file to extract from 
                e.g. "file.csv"
        output: 
            True if the csv was extracted to dataframe and stored in S3, False otherwise
    '''
    return {"result": True} 


@api.post("/v1/extract/excel")
async def extract_excel(file: UploadFile):
    '''
    Extracts an excel file to a dataframe.
        
        inputs: 
            file: excel file to extract from, 
                eg. "file.xls"
        output: 
            True if the excel was extracted to dataframe and stored in S3, False otherwise
    '''
    return {"result": True} 


@api.post("/v1/extract/word")
async def extract_word(file: UploadFile):
    '''
    Extracts an word document to a dataframe. 
        Note: Since this is unstructured data, the text from the file will be extracted and 
        converted to structured vector using Doc2Vec for later analytical processing

        inputs: 
            file: word file to extract from, 
                eg. "file.doc"
        output: 
            True if the document was stored in S3, and the doc vector stored in S3. 
    '''
    return {"result": True} 


@api.post("/v1/extract/pdf")
async def extract_pdf(file: UploadFile):
    '''
    Extracts an pdf document to a dataframe. 
        Note: Since this is unstructured data, the text from the file will be extracted and 
        converted to structured vector using Doc2Vec for later analytical processing.

        inputs: 
            file: pdf file to extract from, 
                eg. "file.pdf"
        output: 
            True if the document was stored in S3, and the doc vector stored in S3. 
    '''
    return {"result": True} 


@api.post("/v1/extract/image")
async def extract_image(file: UploadFile):
    '''
    Extracts an image. 
        Note: for now images are only stored in S3 and meta data stored too in S3.

        inputs: 
            file: image file to extract from, 
                eg. "file.png"
        output: 
            True if the document was stored in S3.
    '''
    return {"result": True} 


@api.post("/v1/extract/video")
async def extract_video(file: UploadFile):
    '''
    Extracts a video.
        Note: for now videos are only stored in S3 and meta data stored too in S3.

        inputs: 
            file: video file to extract from, 
                eg. "file.mov"
        output: 
            True if the video was stored in S3. 
    '''
    return {"result": True} 


##### TRANSFORMATION APIs #####

### TRANSOFRMATION ON DB TABLES
class DBTransforms(BaseModel):
    sql: str
    options: str

@api.post('/v1/transform/db/servers/{server_name}/databases/{database_name}')
async def transform_db(server_name: str, database_name: str, transforms: DBTransforms):
    '''
    Performs transformations on Database tables in the DB staging area

    inputs: transformations as a SQL body
        e.g.: {
            'sql': 'ALTER TABLE Customers DROP COLUMN ContactName;'
        }
    output: status message of transformation
    '''
    return {'result': f'transformations executed successfully'}


### TRANSFORMATION ON PANDAS DATAFRAMES
class PandasTransforms(BaseModel):
    df: str
    op: str
    params: str

@api.post('/v1/transform/df/staging/buckets/{bucket_name}/folders/{folder_name}')
async def transform_df(bucket_name: str, folder_name: str, transforms: PandasTransforms):
    '''
    Performs transformations on dataframes (using pandas) in S3 staging area

    inputs: transformations list of of pandas commands
        e.g.: {
            'dataframe': 'df1',
            'op': 'drop',
            'params': 'A'
        }
    output: status message of transformation
    '''
    return {'result': f'transformations executed successfully'}


##### LOADING APIs #####

# List all DBs in warehouse
@api.get('/v1/load/dw/databases')
def get_dw_databases():
    '''
    Returns databases in the datawarehouse

    inputs:
        None
    outputs:
        list of databases
            e.g.: {'result': '['db1', 'db2']'}
    '''
    # TBD
    return {'result': '["db1", "db2"]'}


# Create DB in warehouse
class DWDatabase(BaseModel):
    database_name: str
    storage_engine: str

@api.post('/v1/load/dw/databases')
async def create_dw_database(database: DWDatabase):
    '''
    Creates a database in the datawarehouse
    
    inputs:
        details of the database to be created including the name
        and storage engine
    output: 
        status message of database creation
    '''
    # TBD
    return {f'database {database.database_name} created successfully'}


# Delete DB in warehouse
@api.delete('/v1/load/dw/databases/{database_name}')
def delete_dw_database(database_name: str):
    '''
    Deletes specified database in data warehouse

    inputs:
        database name
    outputs:
        status message of database deletion
    '''
    return {f'database {database_name} deleted successfully'}


# List all tables in DB in warehouse
@api.get('/v1/load/dw/databases/{database_name}/tables')
def get_dw_tables(database_name: str):
    '''
    Retrieves list of all tables in datawarehouse database

    inputs:
        database name
    outputs:
        list of tables. e.g.:  {'result': '["table1", "table2"]'}
    '''
    #TBD
    return {'result': '["bucket1", "bucket2"]'}


# Create table in DB in warehouse
class DWTable(BaseModel):
    table_name: str

@api.post('/v1/load/dw/databases/{database_name}/tables')
def create_dw_table(database_name: str, table: DWTable):
    '''
    Creates table in datawarehouse

    inputs: 
        table name
    outputs:
        status message of table creation
    '''
    # TBD:
    return {'result': f'table {table.table_name} created successfully'}


# Delete table in DB in warehouse
@api.delete('/v1/load/dw/databases/{database_name}/tables/{table_name}')
def delete_dw_table(database_name: str, table_name: str):
    '''
    Deletes table in datawarehouse

    inputs:
        database name, table name
    output:
        status message of table deletion
    '''
    #TBD
    return {'result': f'table {table_name} deleted successfully'}


# load table in DB in warehouse
class TableData(BaseModel):
    load_file: str
    flags: str

@api.post('/v1/load/dw/databases/{database_name}/tables/{table_name}')
def load_table(database_name: str, table_name:str, table_data:TableData):
    '''
    Loads data into a table in the warehouse. 
    For e.g. it uses the cpimport utility to load a file into 
    MariaDB Columnstore datawarehouse
    
    inputs:
        database name, table name, load file, flags (options for loading)
    outputs:
        status message for data load
    '''
    # TBD
    return {'result': f'data from {table_data.load_file} loaded into table {table_name} successfully'}


##### QUERY #####
class DWQuery(BaseModel):
    sql: str
    options: str

@api.post('/v1/query/dw/databases/{database_name}')
async def query_dw(database_name:str, transforms: DWQuery):
    '''
    Queries datawarehouse database for data required for front end reports

    inputs: transformations as a SQL body
        e.g.: {
            'sql': 'SELECT * FROM Customers;'
        }
    output: query results as a dataframe
    '''
    return {'result': '<json str of dataframe with query result>'}