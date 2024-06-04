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
    # pring("event")
    # pring(event)
    queryParams = event.get('queryStringParameters')
    if queryParams is None or queryParams.get('userId') is None:
        res["statusCode"] = 400
        res["body"] = "No User Id provided"
        return res
    item_count = 0
    # routeKey = event.get('routeKey')
    # pathParameters = event.get('pathParameters')
    # requestBody = event.get('body')
    # queryParams = event.get('queryStringParameters')
    # requestContext = event.get('requestContext')
    # httpRequest = requestContext.get('http')
    # path = httpRequest.get('path')
    # userID = queryParams.get('userId')
    userID = "a8c1d380-5031-7057-3d38-b2f3e002e096"
    responseBody = []
    queryString = "select User.UserID, User.UserName from User join UserFriends on UserFriends.FriendID = User.UserID where UserFriends.UserID = %s"
    with conn.cursor() as cur:
        cur.execute(queryString, (userID))
        responseBody = cur.fetchall()
    conn.commit()
    print(responseBody)
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "body": json.dumps(responseBody),
        "isBase64Encoded": "true"
    }
    print(res)
    return res