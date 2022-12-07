import argparse
import requests


class TokenChecker:
    def __init__(self, new_line):
        self.new_line = new_line
        self.values = self.new_line.replace('(','').replace(')','').split(',')
        self.token_id = self.values[0]
        self.blockchain = self.values[1]
        self.contract_address = self.values[2]
        self.decimals = self.values[3]

    def get_token(self):
        try:
            resp = requests.get("https://api.coinpaprika.com/v1/coins/{}".format(self.token_id))
            resp.raise_for_status()
        except requests.HTTPError as exception:
            print(exception)

parser = argparse.ArgumentParser()
parser.add_argument('--new_line')
args = parser.parse_args()
manager = TokenChecker(new_line=args.new_line)
manager.main()