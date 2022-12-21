import json
import logging

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
                             "avalanche_c": "avax-avalanche",
                             # "arbitrum": "",
                             "gnosis": "gno-gnosis",
                             "optimism": "op-optimism",
                             "ADD MISSING": "CHAINS MAPPINGS HERE"}

    def get_token(self):
        try:
            resp = requests.get("https://api.coinpaprika.com/v1/coins/{}".format(self.token_id))
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError:
            raise


    def validate_token(self):
        #Confirm Symbol
        if self.symbol:
            assert self.token_api_resp[
                       'symbol'] == self.symbol, f"ERROR: {self.token_id} Provided symbol: {self.symbol} does not match CoinPaprika source: {self.token_api_resp['symbol']}"
        else:
            logging.warning(f"WARN: Line: {self.new_line} Symbol is None")

        #Confirm Active
        if self.token_id:
            assert self.token_api_resp['is_active'] == True, f"ERROR: Token: {self.token_id} is not active"
        else:
            logging.warning(f"WARN: Line: {self.new_line} token_id is None")

        #Confirm Contract Listed
        if self.contract_address:
            contracts = [contract['contract'].lower() for contract in self.token_api_resp.get('contracts', [{"contract": "API response missing contracts field"}])]
            assert self.contract_address.lower() in contracts, f"WARN: {self.token_id} Provided contract address: {self.contract_address} not in CoinPaprika contracts list {contracts}. (Not uncommon! share block explorer link to confirm contract)"
        else:
            logging.warning(f"WARN: Line: {self.new_line} contract_address is None")

        #Confirm Platform Matches
        if self.blockchain:
            index_contract = contracts.index(self.contract_address.lower())
            platforms = [contract['platform'] for contract in self.token_api_resp['contracts']]
            platform = platforms[index_contract]
            assert platform == self.chain_mapper.get(self.blockchain,
                                                     self.blockchain), f"ERROR: {self.token_id} Provided blockchain {self.blockchain} does not match expected platform {platform}"
        else:
            logging.warning(f"WARN: Line: {self.new_line} blockchain is None")

