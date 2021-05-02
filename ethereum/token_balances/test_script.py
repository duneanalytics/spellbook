import psycopg2 as pg
import pandas.io.sql as psql
import json
import pandas as pd
import time
import requests

pd.set_option('display.max_columns', None)

def get_random_token_balances_from_db(conn_string_pg):
    """
    Gets random token balances for a particular token from the db
    :return: Pandas dataframe with the token name, address, contract address and balance
    """
    connection = pg.connect(conn_string_pg )
    dataframe = psql.read_sql("""SELECT ts, cast(address as varchar) as address, cast(contract_address as varchar) as contract_address, balance  FROM vasa.balances_per_hour order by RANDOM() LIMIT 10; """, connection)
    return dataframe


def get_etherscan_results(conn_string_pg, api_key):
    """
    Gets balances from Etherscan based on the inputs provided
    api_key - string etherscan api key
    :return: Pandas dataframe with the token name, address, contract address and balance
    """
    dataframe_ = get_random_token_balances_from_db(conn_string_pg)
    print(dataframe_.head())
    dataframe_req = dataframe_[['ts', 'address', 'contract_address']]
    ethscan_list_returns =[]
    for idx, row in dataframe_req.iterrows():
        time.sleep(0.5)
        ct_a = row['contract_address'].replace("\\x", "0x")
        print( "ADDRESS TOKEN", ct_a)
        ad= row['address'].replace("\\x", "0x")
        print("ROW", ad)
        response = requests.get(f"https://api.etherscan.io/api?module=account&action=tokenbalance&contractaddress={ct_a}&address={ad}&tag=latest&apikey={api_key}")
        response_json = json.loads(response.content)
        response_json['ts_r'] = row.ts
        response_json['address_r'] = row['address']
        response_json['contract_address_r'] = row['contract_address']
        ethscan_list_returns.append(response_json )


    df_results = pd.DataFrame.from_records(ethscan_list_returns)
    # df_results['result'] = pd.to_numeric(df_results['result'])/1e18
    check_df = pd.merge(dataframe_, df_results, how='inner', left_on=['ts', 'address', 'contract_address'], right_on=['ts_r','address_r','contract_address_r'])
    print(check_df)
    return check_df



if __name__ == '__main__':
    get_etherscan_results()