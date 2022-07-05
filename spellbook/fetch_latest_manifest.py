import argparse

import requests as re


def fetch_run_ids(token, limit=50):
    headers = {'Content-Type': 'application/json',
               'Authorization': f'Token {token}'}
    response = re.get(f"https://cloud.getdbt.com/api/v2/accounts/58579/runs/?job_definition_id=81672&order_by=-finished_at&limit={limit}", headers=headers)
    run_ids = [elem['id'] for elem in response.json()['data']]
    return run_ids

def loop_till_success(token, run_ids):
    headers = {'Content-Type': 'application/json',
               'Authorization': f'Token {token}'}
    success = False
    while success == False:
        for run_id in run_ids:
            response = re.get(f"https://cloud.getdbt.com/api/v2/accounts/58579/runs/{run_id}/artifacts/manifest.json", headers=headers)
            if response.status_code == 200:
                success = True
                with open('manifest.json', 'w') as out_file:
                    out_file.write(response.text)
                del response
    return run_id

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbt_api_token', type=str)
    args = parser.parse_args()

    token = args.dbt_api_token
    run_ids = fetch_run_ids(token=token)
    loop_till_success(token, run_ids)

if __name__ == "__main__":
    main()