import jwt
import datetime
from locust import HttpUser, task, between


class RecommendationProfile(HttpUser):
    wait_time = between(1, 2)

    def __init__(self, parent):
        super().__init__(parent)

        payload = {
            "sub": "1234567890",
            "name": "world ventures",
            'iat': datetime.datetime.utcnow(),
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        }

        # To Do: Add Config.get_config('env')
        key = ''

        self.token = jwt.encode(payload, key, algorithm='HS256').decode("utf-8")

    @task
    def contacts(self):
        print(self.headers)
        self.client.get("/leads", headers=self.headers)

    def on_start(self):
        resp = self.client.post("/auth", json={
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self.token
        })
        txt = resp.text.replace('"', '').replace('\n', '')

        bearer = f'Bearer {txt}'
        self.headers = {'Authorization': bearer}
