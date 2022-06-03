"""API For deleting user profile picture.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. delete_image_s3(): Function for deleting image to aws S3 bucket
5. handler(): Handling the incoming request with following steps:
- Fetching data from request
- deleting profile picture of the user
- Returning the JSON response with success status code with the message ,authentication token and user_id in the response body
"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3
from botocore.client import Config

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('deletepicture.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

# Variables related to s3 bucket
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')
BUCKET_NAME = environ.get('BUCKET_NAME')
S3_BUCKET_URL = environ.get('S3_BUCKET_URL')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# getting message variable
message_by_language = "165_MESSAGES"

# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.") 

def make_connection():
    """Function to make the database connection."""
    return pymysql.connect(host=endpoint, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)

def log_err(errmsg, status_code):
    """Function to log the error messages."""
    logger.info(errmsg)
    return  {
                "statusCode": status_code,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def jwt_verify(auth_token):
    """Function to verify the authorization token"""
    # decoding the authorization token provided by user
    payload = jwt.decode(auth_token, SECRET_KEY, options={'require_exp': True})
    
    # setting the required values in return
    rid = int(payload['id'])
    user_id = payload['user_id']
    language_id = payload['language_id']
    return rid, user_id, language_id

def delete_image_s3(user_id):
    """Function to delete image to S3"""
    # creating boto3 client 
    S3 = boto3.resource(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET,
        config=Config(signature_version='s3v4')
        )
    response = S3.Object(BUCKET_NAME,user_id + ".png").delete()
    logger.info(response)
    
    # returning response of the delete image
    return response

def handler(event,context):
    """Function to handle the request for delete picture API"""
    global message_by_language
    try:
        # Fetching data from event and rendering it
        auth_token = event['headers']['Authorization']
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['EVENT_DATA_STATUS'], 500)
        
    try:
        # verifying that the user is authorized or not to see this api's data
        rid, user_id, language_id = jwt_verify(auth_token)
    except:
        # if user does not have valid authorization
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['UNAUTHORIZED'], 403)
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['CONNECTION_STATUS'], 500)
        
    try:
        try:
            # Query for getting current language of the user
            selectionQuery = "SELECT `language_id` FROM `users` WHERE `id`=%s"
            # Executing the Query
            cursor.execute(selectionQuery, (rid))
            
            result_list = []
            # fetching result from the cursor
            for result in cursor: result_list.append(result)
            
            # getting current language_id of the user 
            language_id = result_list[0][0]
            message_by_language = str(language_id) + "_MESSAGES"
        except:
            # If there is any error in above operations, logging the error
            return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
            
        try:
            # Query for deleting picture_url of user or setting picture_url to NULL
            updationQuery = "UPDATE `users` SET `picture_url` = NULL, `is_picture_uploaded`=0 WHERE `id`=%s"
            # Executing the Query
            cursor.execute(updationQuery, (rid))
            delete_image_s3(user_id)
        except:
            return log_err (config[message_by_language]['IMAGE_STATUS'], 500)
            
        # returning success json
        return {
                    'statusCode': 200,
                    'headers':{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'
                            },
                    'body': json.dumps({"message":config[message_by_language]['SUCCESS_MESSAGE']})
                }
    except:
        logger.error(traceback.format_exc())
        return log_err (config[message_by_language]['INTERNAL_ERROR'], 500)
        
if __name__== "__main__":
    handler(None,None)