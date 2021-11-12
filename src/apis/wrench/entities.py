import requests
import json
from wrench import Auth
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-e', '--env')
parser.add_argument('-f', '--file')
args = parser.parse_args()

url = f'{Auth.get_host(args.env)}/entity'
api_key = Auth.get(args.env)
params = json.dumps({'source_file': args.file})

response = requests.post(url, data=params, headers={'authorization': api_key, 'content-type': 'application/json'})

print(f'response: {response}')
print(f'response text: {response.text}')
print(f'request headers: {response.request.headers}')
print(f'request body: {response.request.body}')
print(f"curl -X POST {url} -H 'content-type:application/json' -H 'authorization:{api_key}' -d '{params}'")
