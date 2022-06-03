"""API Module to get active notifications count of the user.

It provides the following functionalities:
1. make_connection(): Connecting to the Database using connection details received through environment variables
2. log_err(): Logging error and returning the JSON response with error message & status code
3. jwt_verify(): verifying token and fetching data from the jwt token sent by user
4. handler(): Handling the incoming request with following steps:
- Fetching data required for api
- getting all the notifications that are not visited by user from the database
- sending the success json with the required data i.e. unvisited notifications

"""

import jwt
import json
import pymysql
import logging
import traceback
from os import environ
import configparser

# reading values from property file to get all the response messages
config = configparser.ConfigParser()
config.read('getactivenotifications.properties', encoding = "ISO-8859-1")

# Getting the DB details from the environment variables to connect to DB
endpoint = environ.get('ENDPOINT')
port     = environ.get('PORT')
dbuser   = environ.get('DBUSER')
password = environ.get('DBPASSWORD')
database = environ.get('DATABASE')

# secret keys for data encryption and security token
key = environ.get('DB_ENCRYPTION_KEY')
SECRET_KEY = environ.get('TOKEN_SECRET_KEY')

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

def handler(event,context):
    """Function to handle the request for notifications API"""
    global message_by_language
    try:
        # getting data from the users request
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
        try:
            # query to get notification details of the user
            query = "SELECT COUNT(*), `language_id` FROM `users` WHERE `id`=%s and `user_id`=%s"
            # excecuting the query
            cursor.execute(query, (rid, user_id))
            users_list = []
            # getting results list from cursor
            for result in cursor: users_list.append(result)
            # checking any user with particular rid and user_id exist or not
            count = users_list[0][0]
            
            # getting current language_id of the user
            language_id = users_list[0][1]
            message_by_language = str(language_id) + "_MESSAGES"
            
            if count == 0:
                return log_err(config[message_by_language]['INVALID_USER'], 404)
        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
            
        try:
            try:
                # query to get notification details of the user
                selectionQuery = "SELECT `notification_type`, `json` FROM `notifications` WHERE `rid`=%s AND `visited`=0 ORDER BY `timestamp` DESC"
                # excecuting the query
                cursor.execute(selectionQuery, (rid))
                result_list = []
                # getting results list from cursor and preparing json list
                for result in cursor: result_list.append({"notification_type":result[0],"notification_json":json.loads(result[1])})
                
                # query to set visited status of all notifications when bell icon is clicked
                updationQuery = "UPDATE `notifications` SET `visited`=1 WHERE `rid`=%s AND `visited`=0"
                # excecuting the query
                cursor.execute(updationQuery, (rid))
                
                # preparing success json  with result_list
                return {
                            'statusCode': 200,
                            'headers':{
                                        'Access-Control-Allow-Origin': '*',
                                        'Access-Control-Allow-Credentials': 'true'
                                      },
                            'body': json.dumps(result_list)
                        }
            except:
                logger.error(traceback.format_exc())
                return log_err(config[message_by_language]['EVENT_DATA_STATUS'], 500)

        except:
            logger.error(traceback.format_exc())
            return log_err(config[message_by_language]['INTERNAL_ERROR'], 500)
    except:
        logger.error(traceback.format_exc())
        return log_err(config[message_by_language]['CONNECTION_STATUS'], 500)
        
if __name__== "__main__":
    handler(None,None)

