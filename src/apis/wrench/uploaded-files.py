import requests
import json
from wrench import Auth
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--env')
#parser.add_argument('-f', '--file')
args = parser.parse_args()

url = f'{Auth.get_host(args.env)}/uploaded-files'
api_key = Auth.get(args.env)
#params = json.dumps({'source_file': args.file})

response = requests.get(url, headers={'authorization': api_key})

#print(f'response: {response}')
#print(f'response text: {response.text}')
#print(f'request headers: {response.request.headers}')
#print(f'request body: {response.request.body}')
#print(f"curl {url} -H 'authorization:{api_key}'")

for f in response.json()['files']:
  print(f)
