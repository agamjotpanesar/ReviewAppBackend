import sys
import logging
from package import pymysql
import json
import os
from decimal import Decimal
from datetime import datetime

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
    if queryParams is None or queryParams.get('userId') is None:
        res["statusCode"] = 400
        res["body"] = "No User Id provided"
        return res

    queryString = """
         SELECT 
     m.PlaceID, 
     m.TimeOfMeeting, 
     GROUP_CONCAT(u.UserName) AS userNames,
     p.PlaceName,
    m.OrganiserID,
    m.MeetupID
    
FROM  
    MEETUP m 
JOIN 
    MEETUP_ATTENDEE ma ON m.MeetupID = ma.MeetupID 
JOIN 
    User u ON u.UserID = ma.AttendeeID 
JOIN 
    PLACE p ON p.PlaceID = m.PlaceID
WHERE ma.attending = 1 AND
    m.OrganiserID = %s
GROUP BY 
      m.MeetupID
    """

    userID = queryParams.get('userId')
    # userID = "58416370-e0d1-7018-3570-4a0dd549d684"

    responseBody = []
    with conn.cursor() as cur:
        cur.execute(queryString, (userID))
        responseBody = cur.fetchall()
    conn.commit()
    print(responseBody)
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "body": json.dumps(responseBody,cls=DecimalEncoder),
        "isBase64Encoded": "true"
    }
    print(res)
    return res