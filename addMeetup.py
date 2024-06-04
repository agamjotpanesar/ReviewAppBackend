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
    # print("event")
    # print(event)
    statusCode = 200
    headers = {
        "Content-Type": "application/json"
    }
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "isBase64Encoded": "false"
    }

    data = event.get('body', '{}')
    body = json.loads(data)


    # body = event.get('body', '{}')
    # print('body')
    # print(body)

    # body = event
    # queryParams = event.get('queryStringParameters')
    # if queryParams is None or queryParams.get('meetupid') is None or queryParams.get('organiserid') is None or queryParams.get('attendeeid') is None or queryParams.get('placeid') is None or queryParams.get('timeofmeeting') is None:
    # if body.get('organiserid') is None or body.get('attendeeids') is None or body.get('placeid') is None or body.get('timeofmeeting') is None:
    #     res["statusCode"] = 400
    #     res["body"] = "Parameters not provided"
    #     return res

    organiserId = body.get('organiserid')
    attendeeIds = body.get('attendeeids')
    placeId = body.get('placeid')
    timeOfMeeting = body.get('timeofmeeting')

    # organiserId = "28e10380-c071-7064-a3bb-11a78a0df5bc"
    # attendeeIds = "f8f1f3c0-4001-7056-f23a-61e560a6bad7,28e10380-c071-7064-a3bb-11a78a0df5bc"
    # placeId = "1234566236"
    # timeOfMeeting = "2022-04-22 10:34:53"

    # call CreateMeetupV2('f8f1f3c0-4001-7056-f23a-61e560a6bad7,28e10380-c071-7064-a3bb-11a78a0df5bc', '28e10380-c071-7064-a3bb-11a78a0df5bc', 1234566236, '2022-04-22 10:34:53', @meetupID);
    # queryString = f"INSERT INTO MEETUP (OrganiserID, AttendeeID, PlaceID, TimeOfMeeting) VALUES ('{organiserId}', '{attendeeId}', '{placeId}', '{timeOfMeeting}')"
    args = (attendeeIds, organiserId, placeId, timeOfMeeting)
    print("start args")
    print(attendeeIds)
    print(organiserId)
    print(placeId)
    print(timeOfMeeting)
    print("end args args")
    # print(queryString)
    with conn.cursor() as cur:
        # cur.execute(queryString)
        # stored_procedure = "CALL CreateRecord(%s, %s, @p_id)"
        # cursor.execute(stored_procedure, (name, email))

        # # Fetch the output parameter
        # cursor.execute("SELECT @meetup_id")
        # record_id = cursor.fetchone()[0]
        cur.execute("set @meetup_id = 0")
        stored_procedure = "CALL CreateMeetupV2(%s, %s, %s, %s, @meetup_id)"
        cur.execute(stored_procedure, (attendeeIds, organiserId, placeId, timeOfMeeting))
        cur.execute("SELECT @meetup_id")
        meetup_id = cur.fetchone()[0]
        # meetingId = cur.callproc('CreateMeetupV2', args)
    conn.commit()
    # responseBody = f"\\{'id': {meetup_id}\\}"
    # responseBody = {
    #     "id" : meetup_id
    # }
    # print(responseBody)
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "body": meetup_id,
        "isBase64Encoded": "true"
    }
    print(res)
    return res