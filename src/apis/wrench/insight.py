import requests
import json
from wrench import Auth
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--insight')
parser.add_argument('-e', '--env')
args = parser.parse_args()

# entities = ['5c7a6882-c765-4542-baef-f7cf3becc120', 'e32c97ab-e46f-40a0-9fbe-f72251b236b8']
entities = ['85da6d00-9e64-4913-b66c-63d670d084a7', '0e782bc6-84cb-4c80-bd48-77988ccaaf4c']
url = f'{Auth.get_host(args.env)}/{args.insight}'
api_key = Auth.get(args.env)
params = json.dumps({'entity_id': entities})

response = requests.post(url, data=params, headers={'authorization': api_key, 'content-type': 'application/json'})
print(response.json().get('scores', None))
for i in response.json():
  print(i)

print(f'response: {response}')
print(f'response text: {response.text}')
print(f'request headers: {response.request.headers}')
print(f'request body: {response.request.body}')
print(f"curl -X POST {url} -H 'content-type:application/json' -H 'authorization:{api_key}' -d '{params}'")
