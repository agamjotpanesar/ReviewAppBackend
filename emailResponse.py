import sys
import logging
import pymysql
import json
import os
from decimal import Decimal
from datetime import datetime
import csv

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
    if 'queryStringParameters' not in event or event['queryStringParameters'] is None:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Missing query string parameters'})
        }
    queryParams = event['queryStringParameters']
    # meetupId = queryParams.get('meetupId')
    # attendeeId = queryParams.get('attendeeId')
    # response = queryParams.get('response')


    if queryParams is None or queryParams.get('meetupId') is None or queryParams.get('attendeeId') is None or queryParams.get('response') is None:
        res["statusCode"] = 400
        res["body"] = json.dumps(queryParams)
        return res

    meetupId = queryParams.get('meetupId')
    attendeeId = queryParams.get('attendeeId')
    response = queryParams.get('response')
    queryString = """UPDATE MEETUP_ATTENDEE SET attending = %s where MeetupID = %s and AttendeeID = %s"""
    # print(queryString)
    # responseBody = []
    # with conn.cursor() as cur:
    cur = conn.cursor()
    # cur.execute(queryString, (response, meetupId, attendeeId))
    cur.execute(queryString, (response, meetupId, attendeeId))
    conn.commit()
    cur.close()
    conn.close()


    res = {
        "statusCode": statusCode,
        "headers": headers,
        "body": json.dumps({'message': 'success'}),
        "isBase64Encoded": "true"
    }
    print(res)
    return res

