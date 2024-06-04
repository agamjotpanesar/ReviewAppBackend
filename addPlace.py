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
        "Content-Type": "application/json",

        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
    }
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "isBase64Encoded": "false"
    }
    queryParams = event.get('queryStringParameters')
    body = json.loads(event.get('body'))
    if body is None or body.get('placeid') is None or body.get('placename') is None or body.get('placelongitude') is None or body.get('placelatitude') is None:
        res["statusCode"] = 400
        res["body"] = "Parameters not provided"
        return res

    params = {
        'placeId': body.get('placeid'),
        'placeName': body.get('placename'),
        'placeLongitude': body.get('placelongitude'),
        'placeLatitude': body.get('placelatitude'),
        'placeCity': body.get('placecity')
    }

    queryString = """INSERT INTO PLACE 
                    (PlaceID, PlaceName, PlaceLongitude, PlaceLatitude, Place_City) VALUES 
                    (%(placeId)s, %(placeName)s, %(placeLongitude)s, %(placeLatitude)s, %(placeCity)s)"""
    print(queryString)
    with conn.cursor() as cur:
        cur.execute(queryString, params)
    conn.commit()
    responseBody = f"'{cur.rowcount}' place inserted"
    print(responseBody)
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "body": json.dumps(responseBody),
        "isBase64Encoded": "true"
    }
    print(res)
    return res