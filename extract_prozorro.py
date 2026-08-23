import requests

resp = requests.get('https://api.openprocurement.org/api/2.5/tenders')
if resp.status_code != 200:
    print(f"Error: {resp.status_code}")
else:
    print(f"HTTP status: {resp.status_code}")
    tenders = resp.json()
    data = tenders["data"]
    for i in range(5):
        print(data[i])
