import sys
import logging
from package import pymysql
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

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def serialize_datetime(obj):
    print('obj')
    print(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj
    raise TypeError("Type not serializable")

def parse_response(result):
    print('result in util')
    print(result[1])
    return result


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
    queryParams = event.get('queryStringParameters')
    if queryParams is None or queryParams.get('meetupId') is None:
        res["statusCode"] = 400
        res["body"] = "No Meetup Id provided"
        return res

    meetupId = queryParams.get('meetupId')
    # meetupId = str(20)
    queryString = """select MEETUP.OrganiserID, MEETUP.PlaceID, MEETUP.TimeOfMeeting, MEETUP.MeetupID, GROUP_CONCAT(MEETUP_ATTENDEE.AttendeeID) AS Attendees from MEETUP Join MEETUP_ATTENDEE on (MEETUP.MeetupID = MEETUP_ATTENDEE.MeetupID) where MEETUP.MeetupID = %s"""
    print(queryString)
    responseBody = []
    with conn.cursor() as cur:
        cur.execute(queryString, (meetupId))
        responseBody = cur.fetchall()
    conn.commit()
    json_data=[]
    row_headers=[x[0] for x in cur.description]
    print("row_headers")
    print(row_headers)
    json_data=[]
    for result in responseBody:
        json_data.append(dict(zip(row_headers,result)))
        # json_data.append(parse_response(result))
    # print("json_data")
    # print(json_data)
    parsedResponse = parse_response(json.dumps(json_data, default=serialize_datetime))

    res = {
        "statusCode": statusCode,
        "headers": headers,
        "body": parsedResponse,
        "isBase64Encoded": "true"
    }
    print(res)
    return res