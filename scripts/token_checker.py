import json

import requests


class TokenChecker:
    """
    TokenChecker uses the coinpaprika API, an assertion error does not mean the input data is incorrect, just that
    it is not represented in the coinpaprika API response.
    """
    def __init__(self, new_line):
        self.new_line = new_line
        self.values = json.loads(self.new_line.rstrip(',').replace('(', '[').replace(')', ']'))
        self.token_id = self.values[0]
        self.blockchain = self.values[1]
        self.symbol = self.values[2]
        self.contract_address = self.values[3]
        self.token_api_resp = self.get_token()
        self.chain_mapper = {"ethereum": "eth-ethereum",
                             "bnb": "bnb-binance-coin",
                             "polygon": "matic-polygon",
                             "solana": "sol-solana",
                             "ADD MISSING": "CHAINS MAPPINGS HERE"}

    def get_token(self):
        try:
            resp = requests.get("https://api.coinpaprika.com/v1/coins/{}".format(self.token_id))
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exception:
            print(f"API Call Exception: {exception}")

    def validate_token(self):
        #Confirm Symbol
        assert self.token_api_resp[
                   'symbol'] == self.symbol, f"Line: {self.new_line} Provided symbol: {self.symbol} does not match CoinPaprika source: {self.token_api_resp['symbol']}"

        #Confirm Active
        assert self.token_api_resp['is_active'] == True, f"Line: {self.new_line} Token: {self.token_id} is not active"

        #Confirm Contract Listed
        contracts = [contract['contract'].lower() for contract in self.token_api_resp.get('contracts', [{"contract": "API response missing contracts field"}])]
        assert self.contract_address.lower() in contracts, f"Line: {self.new_line} Provided contract address: {self.contract_address} not in CoinPaprika contracts list {contracts}. (Not uncommon! share block explorer link to confirm contract)"

        #Confirm Platform Matches
        index_contract = contracts.index(self.contract_address.lower())
        platforms = [contract['platform'] for contract in self.token_api_resp['contracts']]
        platform = platforms[index_contract]
        assert platform == self.chain_mapper.get(self.blockchain,
                                                 self.blockchain), f"Line: {self.new_line} Provided blockchain {self.blockchain} does not match expected platform {platform}"

