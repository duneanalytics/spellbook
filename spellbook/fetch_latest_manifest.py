import argparse

import requests as re


def fetch_run_id(token, limit=50):
    headers = {'Content-Type': 'application/json',
               'Authorization': f'Token {token}'}
    response = re.get(f"https://cloud.getdbt.com/api/v2/accounts/58579/runs/?job_definition_id=81672&order_by=-finished_at&limit={limit}", headers=headers)
    last_successful_run_id = [elem['id'] for elem in response.json()['data'] if elem['status_humanized'] == 'Success'][0]
    return last_successful_run_id

def fetch_manifest(token, run_id):
    headers = {'Content-Type': 'application/json',
               'Authorization': f'Token {token}'}
    response = re.get(f"https://cloud.getdbt.com/api/v2/accounts/58579/runs/{run_id}/artifacts/manifest.json", headers=headers)
    if response.status_code == 200:
        with open('manifest.json', 'w') as out_file:
            out_file.write(response.text)
        del response
    else:
        print(response.status_code)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbt_api_token', type=str)
    args = parser.parse_args()

    token = args.dbt_api_token
    run_id = fetch_run_id(token=token)
    fetch_manifest(token, run_id)

if __name__ == "__main__":
    main()