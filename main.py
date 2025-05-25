import requests
import os

def authenticate_and_run():
    sec = os.getenv('ASHRITHA_SECRET_KEY')

    res = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/session',
                        headers={"Content-Type": "application/json"},
                        json={"username": 'ashritha.k@newtonschool.co', "password": sec})
    
    if res.ok:
        token = res.json()['id']
        print(f"Success: Token {token}")
    else:
        print("Failed to authenticate")

if __name__ == "__main__":
    authenticate_and_run()
