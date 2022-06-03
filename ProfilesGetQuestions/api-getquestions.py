#!/usr/bin/env python3

"""API Module to provide Fetching Questions Functionalities.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. handler(): Handling the incoming request with following steps:
- Fetching the questions 
- Returning the JSON response with list of questions and success status code

"""

import json
import pymysql
import logging
import traceback
from os import environ
import configparser
import boto3

# For getting messages according to language of the user
message_by_language = "165_MESSAGES"

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getbig5.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# aws cridentials required for creating boto3 client object
AWS_REGION = environ.get('REGION')
AWS_ACCESS_KEY = environ.get('ACCESS_KEY_ID')
AWS_SECRET = environ.get('SECRET_ACCESS_KEY')
ENVIRONMENT_TYPE = environ.get('ENVIRONMENT_TYPE')

#Logger key
logging_Level = int(environ.get('LOGGING_LEVEL'))
# Getting the logger to log the messages for debugging purposes
logger   = logging.getLogger()
# Setting the log level to INFO
logger.setLevel(logging_Level)

logger.info("Cold start complete.") 

def make_connection():
    """Function to make the database connection."""
    return pymysql.connect(host=endpoint, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)
        
def make_client():
    """Making a boto3 aws client to perform invoking of functions"""
    
    # creating an aws client object by providing different cridentials
    invokeLam = boto3.client(
                                "lambda", 
                                region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET
                            )
    # returning the object
    return invokeLam

def log_err(errmsg):
    """Function to log the error messages."""
    return  {
                "statusCode": 500,
                "body": json.dumps({"message":errmsg}) , 
                "headers":{
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Credentials': 'true'}, 
                "isBase64Encoded":"false"
            }

def handler(event,context):
    """Function to handle the request for Get Big5 API."""
    global message_by_language
    try:
        logger.info(event)
        # checking that the following event call is from lambda warmer or not
        if event['source']=="lambda_warmer":
            logger.info("lambda warmed")
            # returning the success json
            return {
                       'status_code':200,
                       'body':{"message":"lambda warmed"}
                   }
    except:
        # If there is any error in above operations
        pass
    
    try:
        # fetching language_id from the event data
        language_id = event['headers']['language_id']
        if language_id == "null":
            try:
                # making an boto 3 client object
                invokeLam = make_client()
                # invoking the lambda function with custom payload
                response = invokeLam.invoke(FunctionName= "ProfilesGetLanguage" + ENVIRONMENT_TYPE, InvocationType="RequestResponse", Payload=json.dumps({"headers":{"Accept-Language":event['headers']['Accept-Language']}}))
                response = response['Payload']
                response = json.loads(response.read().decode("utf-8"))
                # gettin language_id from response
                language_id = json.loads(response['body'])['language_id']
            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['INVOCATION_ERROR'])
                
        messsage_by_language = str(language_id) + "_MESSAGES"
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['EVENT_DATA_STATUS'])
        
    try:
        # Making the DB connection
        cnx    = make_connection()
        # Getting the cursor from the DB connection to execute the queries
        cursor = cnx.cursor()
        
        try:
            # Constructing the query to get language_id according to country code
            selectionQuery = "SELECT COUNT(*) FROM `users` WHERE `is_active`=1"
            cursor.execute(selectionQuery)
            try:
                ans_list = []
                for result in cursor: ans_list.append(result)
                total_user_count = ans_list[0][0]
            except:
                # If there is any error in above operations, logging the error
                logger.error(traceback.format_exc())
                return log_err (config['MESSAGES']['TOTAL_USER_COUNT'])
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config['MESSAGES']['QUERY_EXECUTION_STATUS'])
            
        try:
            # Getting questions according to the language id
            if int(language_id)==165:
                # Constructing query to fetch questions
                query    = "SELECT `id`,`question` FROM `questions_120` WHERE `language_id`=%s"
                # Executing the query using cursor
                cursor.execute(query, (language_id))
            else:
                # Constructing query to fetch questions
                query    = "SELECT `question_id`,`question` FROM `questions_120_translations` WHERE `language_id`=%s"
                # Executing the query using cursor
                cursor.execute(query, (language_id))
        except:
            # If there is any error in above operations, logging the error
            logger.error(traceback.format_exc())
            return log_err (config[message_by_language]['QUERY_EXECUTION_STATUS'])
        
        try:
            results_list=[]
            # Iterating through all results and preparing a list
            for result in cursor: results_list.append({"id":result[0],"question":result[1]})
            results_list[0]
            # Returning JSON response           
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps({"questions":results_list, "total_user_count":total_user_count, "language_id":int(language_id)})
            }
        except:
            # If there is any error in above operations, logging the error
            return {
                    'statusCode': 200,
                    'headers': {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Credentials': 'true'
                    },
                    'body': json.dumps({"message":config[message_by_language]['QUESTIONS_STATUS']})
                }
    except:
        # If there is any error in above operations, logging the error
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'])
    finally:
        try:
            # Finally, clean up the connection
            cursor.close()
            cnx.close()
        except: 
            pass

if __name__== "__main__":
    handler(None,None)