import sys
import logging
from package import pymysql
import json
import os
import base64

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


class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            temp = int.from_bytes(obj, "big")
            print(temp)
            return temp
        return super().default(obj)

def lambda_handler(event, context):
    print(event)
    statusCode = 200
    headers = {
        "Content-Type": "application/json"
    }
    item_count = 0
    responseBody = []
    with conn.cursor() as cur:
        cur.execute("select * from REVIEW")
        responseBody = cur.fetchall()
    conn.commit()
    print(responseBody)
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "body": json.dumps(responseBody, cls=BytesEncoder),
        "isBase64Encoded": "false"
    }
    print(res)
    return res