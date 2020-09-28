import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from botocore.exceptions import ClientError

# Specify a configuration set. If you do not want to use a configuration
# set, comment the following variable, and the
# ConfigurationSetName=CONFIGURATION_SET argument below.
#CONFIGURATION_SET = "ConfigSet"

# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
AWS_REGION = "us-east-1"

# The character encoding for the email.
CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
client = boto3.client('ses', region_name=AWS_REGION)

def sendEmailForACampaign(emailBody, htmlBody, emailSubject, emailRecipients, emailBccRecipients, emailFrom):
  # Try to send the email.
  try:
      # Provide the contents of the email.
      response = client.send_email(
          Destination={
              'ToAddresses': emailRecipients,
              'BccAddresses': emailBccRecipients
          },
          Message={
              'Body': {
                  'Html': {
                      'Charset': CHARSET,
                      'Data': htmlBody,
                  },
                  'Text': {
                      'Charset': CHARSET,
                      'Data': emailBody,
                  },
              },
              'Subject': {
                  'Charset': CHARSET,
                  'Data': emailSubject,
              },
          },
          Source=emailFrom,
          # If you are not using a configuration set, comment or delete the
          # following line
          # ConfigurationSetName=CONFIGURATION_SET,
      )
  # Display an error if something goes wrong.
  except ClientError as e:
      print(e.response['Error']['Message'])
  else:
      print("Email sent! Message ID:"),
      print(response['MessageId'])


def sendTextEmail(emailBody, emailSubject, emailRecipients, emailBccRecipients, emailFrom):
  try:
      response = client.send_email(
          Destination={
              'ToAddresses': emailRecipients,
              'BccAddresses': emailBccRecipients
          },
          Message={
              'Body': {
                  'Text': {
                      'Charset': CHARSET,
                      'Data': emailBody,
                  },
              },
              'Subject': {
                  'Charset': CHARSET,
                  'Data': emailSubject,
              },
          },
          Source=emailFrom,
          # If you are not using a configuration set, comment or delete the
          # following line
          # ConfigurationSetName=CONFIGURATION_SET,
      )
  except ClientError as e:
      print(e.response['Error']['Message'])
  else:
      print("Email sent! Message ID:"),
      print(response['MessageId'])


def sendRawEmail(emailBody, emailSubject, emailRecipients, emailBccRecipients, emailFrom, attachment):
    CONFIGURATION_SET = "ConfigSet"
    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    # Add subject, from and to lines.
    msg['Subject'] = emailSubject 
    msg['From'] = emailFrom 
    msg['To'] = emailRecipients

    # Create a multipart/alternative child container.
    msg_body = MIMEMultipart('alternative')

    # Encode the text and HTML content and set the character encoding. This step is
    # necessary if you're sending a message with characters outside the ASCII range.
    textpart = MIMEText(emailBody.encode(CHARSET), 'plain', CHARSET)
    # htmlpart = MIMEText(BODY_HTML.encode(CHARSET), 'html', CHARSET)

    # Add the text and HTML parts to the child container.
    msg_body.attach(textpart)
    # msg_body.attach(htmlpart)

    # Define the attachment part and encode it using MIMEApplication.
    att = MIMEApplication(open(attachment, 'rb').read())
    # att = MIMEApplication(attachment.read())
    # att = MIMEApplication(attachmentText)

    # Add a header to tell the email client to treat this part as an attachment,
    # and to give the attachment a name.
    att.add_header('Content-Disposition','attachment',filename='test.txt')

    # Attach the multipart/alternative child container to the multipart/mixed
    # parent container.
    msg.attach(msg_body)

    # Add the attachment to the parent container.
    msg.attach(att)

    try:
        #Provide the contents of the email.
        response = client.send_raw_email(
            Source=emailFrom,
            Destinations=[
                emailRecipients
            ],
            RawMessage={
                'Data':msg.as_string(),
            },
            ConfigurationSetName=CONFIGURATION_SET
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
