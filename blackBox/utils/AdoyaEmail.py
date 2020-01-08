import boto3
from botocore.exceptions import ClientError

# Specify a configuration set. If you do not want to use a configuration
# set, comment the following variable, and the
# ConfigurationSetName=CONFIGURATION_SET argument below.
#CONFIGURATION_SET = "ConfigSet"

# If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
AWS_REGION = "us-east-1"

# The subject line for the email.
# SUBJECT = "Amazon SES Test (SDK for Python)"

# The email body for recipients with non-HTML email clients.
# BODY_TEXT = ("Amazon SES Test (Python)\r\n"
#              "This email was sent with Amazon SES using the "
#              "AWS SDK for Python (Boto)."
#              )

# The HTML body of the email.
# BODY_HTML = """<html>
# <head></head>
# <body>
#   <h1>Amazon SES Test (SDK for Python)</h1>
#   <p>This email was sent with
#     <a href='https://aws.amazon.com/ses/'>Amazon SES</a> using the
#     <a href='https://aws.amazon.com/sdk-for-python/'>
#       AWS SDK for Python (Boto)</a>.</p>
# </body>
# </html>
#             """

# The character encoding for the email.
CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
client = boto3.client('ses', region_name=AWS_REGION)

def sendEmailForACampaign(emailBody, emailSubject, emailRecipients, emailFrom):
  # Try to send the email.
  try:
      # Provide the contents of the email.
      response = client.send_email(
          # Destination={
          #     'ToAddresses': [
          #         RECIPIENT,
          #     ],
          # },
          Destination={
              'ToAddresses': emailRecipients
          },
          Message={
              'Body': {
                  # 'Html': {
                  #     'Charset': CHARSET,
                  #     'Data': emailBody,
                  # },
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

