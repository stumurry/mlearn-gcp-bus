import requests
import gzip
from wrench import Auth
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('-e', '--env')
args = parser.parse_args()

file = 'staging.zleads-bq-to-wrench-62fd39c4-e447-48e8-aba8-e1390c888494-00000-of-00001.csv.gz'
files = {'file': (file.replace('.gz', ''), gzip.open(file, 'rb'))}
response = requests.put(f'{Auth.get_host(args.env)}/upload', files=files, headers={'authorization': Auth.get(args.env)})

print(f'request headers: {response.request.headers}')
print(f'request body: {response.request.body}')
print(f'response: {response}')
print(f'response text: {response.text}')
