import datetime
import os
import requests
import prefect

from prefect import task, Flow
from prefect.client.secrets import Secret
from prefect.engine import signals
from prefect.environments.storage import Docker
from prefect.schedules import IntervalSchedule
from twilio.rest import Client as TwilioClient

# ----------------------------------------------------------------
# Before the Run
# ----------------------------------------------------------------

# Before doing anything, make sure that the DOCKER_REGISTRY
# environment variable is set to your docker registry, which is
# typically just your Docker Hub username.
#
# Local Runs
#
# Update [context.secrets] in your ~/prefect/config.toml with
# the following values:
#
# NBA_API_KEY - API key for NBA stats from Rapid API
# NBA_API_PHONE_NUMBER - The phone number that will receive texts
# NBA_API_TWILIO_ACCOUNT_SID
# NBA_API_TWILIO_AUTH_TOKEN
# NBA_API_TWILIO_PHONE_NUMBER - The phone number that will send texts

# Cloud Runs
#
# Ensure that the secrets above are set in your Cloud account.
# Ensure that you're logged into Cloud via CLI.
# Once you log in, execute `poetry run flow-deploy` to deploy to
# Cloud.

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

  logger = prefect.context.get('logger')

  if r.status_code != 200:
    message = f'Received a {r.status_code} from the NBA API.'
    logger.error(message)
    raise signals.FAIL(message=message)

  result =  r.json()['api']['games']
  logger.debug(result)
  return result

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
  elif len(close_games) == 1:
    game = close_games[0]
    s = f"""
      There is a close NBA game on right now between the {game['vTeam']['nickName']}
      and {game['hTeam']['nickName']}! Score:
      {game['vTeam']['score']['points']}-{game['hTeam']['score']['points']}.
    """
    return " ".join(s.split())
  else:
    s = f"""
      There are {len(close_games)} close NBA games on right now!
      {
        " ".join([
          f"{game['vTeam']['nickName']} @ {game['hTeam']['nickName']}: {game['vTeam']['score']['points']}-{game['hTeam']['score']['points']}."
          for game in close_games
        ])
      }
    """
    return " ".join(s.split())

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

schedule = IntervalSchedule(
  interval=datetime.timedelta(minutes=15)
)

with Flow("Close NBA Games", schedule) as f:
  games = fetch_nba_games(nba_api_key = Secret('NBA_API_KEY'))

  games = check_for_closeness.map(games)

  message = compose_message(games)

  send_message(
    account_sid = Secret('NBA_API_TWILIO_ACCOUNT_SID'),
    auth_token = Secret('NBA_API_TWILIO_AUTH_TOKEN'),
    message = message,
    receiver = Secret('NBA_API_PHONE_NUMBER'),
    sender = Secret('NBA_API_TWILIO_PHONE_NUMBER')
  )

# ----------------------------------------------------------------
# Local Run
# ----------------------------------------------------------------

def run():
  f.run()

# ----------------------------------------------------------------
# Cloud Deploy
# ----------------------------------------------------------------

def deploy():
  f.storage = Docker(
    registry_url=os.environ['DOCKER_REGISTRY'],
    python_dependencies=["twilio"]
  )
  f.register(project_name="Pramod's Flows")
