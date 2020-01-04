import requests

import prefect
from prefect import task, Flow
from prefect.client.secrets import Secret
from prefect.engine import signals
from prefect.environments.storage import Docker

from twilio.rest import Client as TwilioClient

# ----------------------------------------------------------------
# Before the Run
# ----------------------------------------------------------------

# Local Runs
#
# Update [context.secrets] in your ~/prefect/config.toml with the
# following values:
#
# NBA_API_KEY - API key for NBA stats from Rapid API
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
def fetch_nba_games(nba_api_key):
  r = requests.get(
    'https://api-nba-v1.p.rapidapi.com/games/live/',
    headers={
      'x-rapidapi-host': 'api-nba-v1.p.rapidapi.com',
      'x-rapidapi-key': nba_api_key.get()
    }
  )

  if r.status_code != 200:
    message = f'Received a {r.status_code} from the NBA API.'
    prefect.context.get('logger').error(message)
    raise signals.FAIL(message=message)

  return r.json()['api']['games']

@task
def check_for_closeness(game):
  is_close = game['currentPeriod'] == "4/4" and abs(
      int(game['vTeam']['score']['points']) -
      int(game['hTeam']['score']['points'])
    ) < 4

  return { **game, 'isClose': is_close }

@task
def compose_message(games):
  is_close = lambda game: game['isClose']

  close_games = [game for game in filter(is_close, games)]

  if len(close_games) == 0:
    return None

  return "There is a close NBA game on right now!"

@task
def send_message(account_sid, auth_token, message, sender, receiver):
  if message is None:
    return

  twilio_client = TwilioClient(account_sid.get(), auth_token.get())

  twilio_client.messages.create(
    from_=sender.get(),
    to=receiver.get(),
    body=message
  )

# ----------------------------------------------------------------
# Flow
# ----------------------------------------------------------------

with Flow("Close NBA Games") as f:
  games = fetch_nba_games(nba_api_key = Secret('NBA_API_KEY'))

  games = check_for_closeness.map(games)

  message = compose_message(games)

  send_message(
    account_sid = Secret('TWILIO_ACCOUNT_SID'),
    auth_token = Secret('TWILIO_AUTH_TOKEN'),
    message = message,
    receiver = Secret('PHONE_NUMBER'),
    sender = Secret('TWILIO_PHONE_NUMBER')
  )

# ----------------------------------------------------------------
# Local Run
# ----------------------------------------------------------------

def run():
  f.run()

def deploy():
  f.storage = Docker()
  f.register(
    project_name="Hello, World!",
    # registry_url='',
    python_dependencies=["twilio"]
  )
