import requests
import json

class Auth:
    
    @classmethod
    def get_credentials(cls, env):
        config = {
          'prd': {'username': 'plexisprod@wrench.ai', 'password': 'VT6ryDP@V*mqpB7Y'},
          'qa': {'username': 'plexsqa@wrench.ai', 'password': '!Q964r.*6*CHy8ib'},
          'dev': {'username': 'plexisdev@wrench.ai', 'password': 'kdvB7F-Mkm4mBbrw'},
          'local': {}
        }
        return config.get(env, None)

    @classmethod
    def get_host(cls, env):
      config = {
        'prd': f'https://api.web.wrench.ai',
        'qa': f'https://qa-api.web.wrench.ai',
        'dev': f'https://dev-api.web.wrench.ai',
        'local': f'http://localhost:8080'
      }
      return config.get(env, None)

    @classmethod
    def get(cls, env):
        response = requests.post(f'{cls.get_host(env)}/generate-api-key', data=json.dumps(cls.get_credentials(env)), headers={'content-type': 'application/json'})
        if (response.status_code == 200):
            res_json = response.json()
            return res_json['api_key']
        else:
            print(f'request headers: {response.request.headers}')
            print(f'request body: {response.request.body}')
            print(f'response: {response}')
            print(f'response text: {response.text}')
            raise Exception(f'Authentication failed: {response.text}')
