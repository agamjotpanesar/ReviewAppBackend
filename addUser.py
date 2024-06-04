import sys
import logging
from package import pymysql
import json
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_db_config(filename='config.properties'):
    db_config = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Ignore empty lines and comments
                key, value = line.split('=')
                db_config[key.strip()] = value.strip()
    return db_config

try:
    dbCreds = read_db_config()
    host = dbCreds.get('host')
    user = dbCreds.get('user')
    password = dbCreds.get('password')
    databaseName = dbCreds.get('database')
    conn = pymysql.connect(host=host, user=user, passwd=password, db=databaseName, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit(1)

logger.info("SUCCESS: Connection to RDS for MySQL instance succeeded")

def lambda_handler(event, context):
    print(event)
    statusCode = 200
    headers = {
        "Content-Type": "application/json"
    }
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "isBase64Encoded": "false"
    }
    print(event['request']['userAttributes']['sub'],
          event['request']['userAttributes']['email'],
          event['request']['userAttributes']['custom:Username'],
          event['request']['userAttributes']['custom:City'])

    params = {
        'userId': event['request']['userAttributes']['sub'],
        'userName': event['request']['userAttributes']['custom:Username'],
        'userEmail': event['request']['userAttributes']['email'],
        'defaultCity': event['request']['userAttributes']['custom:City']
    }
    queryString = """INSERT INTO User 
                    (UserID, UserName, UserEmail, DefaultCity) VALUES 
                    (%(userId)s, %(userName)s, %(userEmail)s, %(defaultCity)s)"""
    print(queryString)
    with conn.cursor() as cur:
        cur.execute(queryString, params)
    conn.commit()
    responseBody = f"'{cur.rowcount}' user inserted"
    print(responseBody)
    return event