import boto3
from botocore.exceptions import ClientError
import json


def lambda_handler(event, context):
    # Replace sender@example.com with your "From" address.
    # This address must be verified with Amazon SES.
    # SENDER = event['senderName'] + " <mail@breezingreview.xyz>"
    SENDER =  "Breezing Review <mail@breezingreview.xyz>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    # RECIPIENT = event['recipientEmail']

    # RECIPIENT_ID = event['recipientId']

    # SENDER_ID = event['senderId']

    PLACE_NAME = event['placeName']

    AWS_REGION = "us-west-2"

    queryString = """select m.OrganiserID, m.TimeOfMeeting, GROUP_CONCAT(u.UserEmail) as attendeeEmails, GROUP_CONCAT(u.UserName) as attendeeNames FROM MEETUP m JOIN MEETUP_ATTENDEE ma ON m.MeetupID = ma.MeetupID JOIN User u on ma.AttendeeID = u.UserID where m.MeetupID = 115"""

    # The subject line for the email.
    # SUBJECT = "Hello "+ RECIPIENT_NAME + "! " + SENDER_NAME + "would like to schedule a meetup with you."
    SUBJECT = "Hello there"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("Amazon SES Test (Python)\r\n"
                 "This email was sent with Amazon SES using the "
                 "AWS SDK for Python (Boto)."
                 )


    CHARSET = "UTF-8"


    # templateData = json.dumps(event["templateData"])
    templateData = {
        "senderName": "John",
        "recipientName": "Mike",
        "placeName": "UW Tacoma",
        "meetingTime": "5 pm tomorrow",
        "acceptLink": "https://www.amazon.com",
        "rejectLink": "https:www.google.com"
    }

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_templated_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            ReplyToAddresses=[
                'noreply@breezingreview.xyz',
            ],
            Source=SENDER,
            Template='MeetingInvite',
            TemplateData=templateData
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])