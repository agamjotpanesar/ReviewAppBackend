import boto3
from botocore.exceptions import ClientError
import json
import pymysql
import logging
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

def serialize_datetime(obj):
    print('obj')
    print(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj
    raise TypeError("Type not serializable")


def lambda_handler(event, context):
    statusCode = 200
    headers = {
        "Content-Type": "application/json"
    }
    res = {
        "statusCode": statusCode,
        "headers": headers,
        "isBase64Encoded": "false"
    }
    # Replace sender@example.com with your "From" address.
    # This address must be verified with Amazon SES.
    # SENDER = event['senderName'] + " <mail@breezingreview.xyz>"
    print('event')
    print(event)
    body = json.loads(event.get('body'))


    try:
        if (body['placeName']) and (body['placeName'] is not None):
            placeName = body['placeName']
            meetupId = body['meetupId']
    except KeyError:
        print('Incorrect body passed')
    #     res["statusCode"] = 400
    #     res["body"] = "No Meetup Id provided"
    #     return res
    # meetupId = queryParams.get('meetupId')

    # print("body")
    # print(body)
    # placeName = body.get('placeName')
    # meetupId = body.get('meetupId')

    print('meetupId')
    print(meetupId)
    print('placeName')
    print(placeName)

    # placeName = "Tacoma"
    # meetupId = 124
    SENDER =  "Breezing Review <mail@breezingreview.xyz>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    # RECIPIENT = event['recipientEmail']

    # RECIPIENT_NAME = event['recipientName']

    # SENDER_NAME = event['senderName']

    AWS_REGION = "us-west-2"

    # The subject line for the email.
    # SUBJECT = "Hello "+ RECIPIENT_NAME + "! " + SENDER_NAME + "would like to schedule a meetup with you."
    # SUBJECT = "Hello there"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("Amazon SES Test (Python)\r\n"
                 "This email was sent with Amazon SES using the "
                 "AWS SDK for Python (Boto)."
                 )


    CHARSET = "UTF-8"

    # templateData = json.dumps(event["templateData"])

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)

    queryString = """select (select u.UserName from User u JOIN MEETUP m ON m.OrganiserID = u.UserID where m.MeetupID = %s ) as OrganiserName, m.TimeOfMeeting, GROUP_CONCAT(u.UserID) as attendeeIds, GROUP_CONCAT(u.UserEmail) as attendeeEmails, GROUP_CONCAT(u.UserName) as attendeeNames FROM MEETUP m JOIN MEETUP_ATTENDEE ma ON m.MeetupID = ma.MeetupID JOIN User u on ma.AttendeeID = u.UserID where m.MeetupID = %s"""

    # Try to send the email.
    try:
        with conn.cursor() as cur:
            cur.execute(queryString, (meetupId, meetupId,))
            responseBody = cur.fetchall()
        conn.commit()
        json_data=[]
        row_headers=[x[0] for x in cur.description]
        print("row_headers")
        print(row_headers)
        json_data=[]
        for result in responseBody:
            json_data.append(dict(zip(row_headers,result)))
        # parsedResponse = json.dumps(json_data, default=serialize_datetime)
        print("json_data")
        print(json_data)

        attendeeEmails = json_data[0]["attendeeEmails"].split(',')
        meetingTime = json_data[0]["TimeOfMeeting"].strftime("%A, %B %d, %-I:%M%p")
        attendeeNames = json_data[0]["attendeeNames"].split(',')
        attendeeIds = json_data[0]["attendeeIds"].split(',')
        senderName = json_data[0]["OrganiserName"]

        print("attendeeEmails")
        print(attendeeEmails)
        responses = []
        meetupIdString = str(meetupId)

        for i, email in enumerate(attendeeEmails):
            acceptLink = "https://xwafhx6kn1.execute-api.us-west-2.amazonaws.com/prod/email/response?meetupId=" + meetupIdString + "&attendeeId=" + attendeeIds[i] + "&response=1"
            rejectLink = "https://xwafhx6kn1.execute-api.us-west-2.amazonaws.com/prod/email/response?meetupId=" + meetupIdString + "&attendeeId=" + attendeeIds[i] + "&response=0"
            RECIPIENT=email
            templateData= {
                "senderName": senderName,
                "recipientName": attendeeNames[i],
                "placeName": placeName,
                "meetingTime": meetingTime,
                "acceptLink": acceptLink,
                "rejectLink": rejectLink
            }
            response = client.send_templated_email(
                Source=SENDER,
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                ReplyToAddresses=[
                    'noreply@breezingreview.xyz',
                ],
                Template='MeetingInvite',
                TemplateData=json.dumps(templateData)
            )
            print(response)
            responses.append(response)


        res = {
            "statusCode": statusCode,
            "headers": headers,
            "body": json.dumps(responses),
            "isBase64Encoded": "true"
        }

        # Provide the contents of the email.
        # response = client.send_templated_email(
        #     Destination={
        #         'ToAddresses': [
        #             RECIPIENT,
        #         ],
        #     },
        #     ReplyToAddresses=[
        #         'noreply@breezingreview.xyz',
        #     ],
        #     Source=SENDER,
        #     Template='MeetingInvite',
        #     TemplateData=templateData
        # )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(res)
        return res