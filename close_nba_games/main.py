from prefect import context, task, triggers, Flow, Parameter
from prefect.client.secrets import Secret
from twilio.rest import Client as TwilioClient

# ----------------------------------------------------------------
# Before the Run
# ----------------------------------------------------------------

# Local Runs
#
# Update [context.secrets] in your ~/prefect/config.toml with the
# following values:
#
# PHONE_NUMBER - The phone number that will receive texts
# TWILIO_ACCOUNT_SID
# TWILIO_AUTH_TOKEN
# TWILIO_PHONE_NUMBER - The phone number that will send texts

# Cloud Runs
#
# Ensure that the secrets above are stored in your Cloud account

# ----------------------------------------------------------------
# Tasks
# ----------------------------------------------------------------

@task
def create_twilio_client(twilio_account_sid, twilio_auth_token):
  return TwilioClient(
    twilio_account_sid.get(),
    twilio_auth_token.get()
  )

@task
def send_reminder(phone_number, twilio_client, twilio_phone_number):
  twilio_client.messages.create(
    from_=twilio_phone_number.get(),
    to=phone_number.get(),
    body="Hello!"
  )

# ----------------------------------------------------------------
# Flow
# ----------------------------------------------------------------

with Flow("Close NBA Games") as f:
  # Create Twilio client
  twilio_client = create_twilio_client(
    twilio_account_sid = Secret('TWILIO_ACCOUNT_SID'),
    twilio_auth_token = Secret('TWILIO_AUTH_TOKEN')
  )

  # Send SMS
  result = send_reminder(
    phone_number = Secret('PHONE_NUMBER'),
    twilio_client = twilio_client,
    twilio_phone_number = Secret('TWILIO_PHONE_NUMBER')
  )

# ----------------------------------------------------------------
# Local Run
# ----------------------------------------------------------------

def run():
  f.run()
