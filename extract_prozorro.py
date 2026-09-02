import requests as rq
import re

host = 'https://api.openprocurement.org'
endpnt = host+'/api/0/tenders'
cpv = r"336\d{2}000-\d{1}"  # CPV code to check for


# host = 'https://lb-api-sandbox-2.prozorro.gov.ua/api/2.5'

# def get_feed():
#   return gross_lst

def get_tender(tnd_id):
    resp = rq.get(f"{endpnt}/{tnd_id}")
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return False
    return resp.json()

def check_cpv(tnd_data, cpv):
    for i in tnd_data["data"]["items"]:
        print(f"Checking item: {i['description']}, CPV: {i['classification']['id']}")
        if re.match(cpv, i["classification"]["id"]):
            return True
    return False

# print(check_cpv(get_tender("92009b870611404dbe0aada6f5acd3b5"), cpv))

# todo: create function to process page of tenders
resp = rq.get(f"{endpnt}?descending=1&limit=10")
if resp.status_code != 200:
    print(f"Error: {resp.status_code}")
else:
    # print(f"HTTP status: {resp.status_code}")
    feed = resp.json()
    data = feed["data"]
    for i in data:
        tnd_id = i["id"]
        tnd_data = get_tender(tnd_id)
        if tnd_data:
            if check_cpv(tnd_data, cpv):
                print(f"Tender ID: {tnd_id}")
                # todo: save tender data to file
            else:
                print(f"Tender ID: {tnd_id} does not match CPV {cpv}")

# todo: create function to process all pages of tenders


