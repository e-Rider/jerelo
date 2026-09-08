import json
import re
import time
import requests as rq

# Константи конфігурації
HOST = "https://api.openprocurement.org"
ENDPOINT = f"{HOST}/api/0/tenders"
CPV_PATTERN = r"^336\d{2}000-\d{1}$"  # CPV-коди медичних препаратів
STORE_PATH = "raw_tenders.jsonl" #JSON Lines

def save_tender_data(tnd_data):
    with open(STORE_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(tnd_data, ensure_ascii=False) + "\n")

def get_tender(tnd_id):
    resp = rq.get(f"{ENDPOINT}/{tnd_id}")
    if resp.status_code != 200:
        print(f"Error: {resp.status_code}")
        return False
    return resp

def check_cpv(tnd_data, cpv):
    for i in tnd_data["data"]["items"]:
        # print(f"Checking item: {i['description']}, CPV: {i['classification']['id']}")
        if re.match(cpv, i["classification"]["id"]):
            return True
    return False

# todo: create function to process page of tenders
def walk_last_tenders():
    limit = 10
    pages = 5
    current_page = 1
    next_page_uri = f"{ENDPOINT}?descending=1&limit={limit}"

    while current_page <= pages:
        resp = rq.get(next_page_uri)
        if resp.status_code != 200:
            print(f"Error: {resp.status_code}")
        else:
            # print(f"HTTP status: {resp.status_code}")
            feed = resp.json()
            data = feed["data"]
            if len(data) == 0:
                print("No more tenders found.")
                break
            for i in data:
                tnd_id = i["id"]
                time.sleep(0.01)
                tnd_data = get_tender(tnd_id)
                if tnd_data:
                    if check_cpv(tnd_data.json(), CPV_PATTERN):
                        print(f"Tender ID: {tnd_id}")
                        save_tender_data(tnd_data.json())
                        # todo: save tender data to file
                    else:
                        # print(f"Tender ID: {tnd_id} does not match CPV {cpv}")
                        pass

            next_page_uri = feed["next_page"]["uri"]
            current_page += 1
            time.sleep(0.5)  # delay to avoid hitting the API too quickly

if __name__ == "__main__":
    with open(STORE_PATH, "w") as f:
        f.write("")
    walk_last_tenders()


