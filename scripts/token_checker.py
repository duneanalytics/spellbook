import json
import logging
import re

import requests


class TokenChecker:
    """
    TokenChecker uses the coinpaprika API, an assertion error does not mean the input data is incorrect, just that
    it is not represented in the coinpaprika API response.
    """

    def __init__(self):
        self.chain_slugs = {"ethereum": "eth-ethereum",
                            "bnb": "bnb-binance-coin",
                            "polygon": "matic-polygon",
                            "solana": "sol-solana",
                            "avalanche_c": "avax-avalanche",
                            "arbitrum": "arb-arbitrum",
                            "gnosis": "gno-gnosis",
                            "optimism": "op-optimism",
                            "fantom": "ftm-fantom",
                            "celo": "celo-celo",
                            "base": "base-base",
                            "zksync": "zksync-zksync",
                            "zora": "eth-ethereum",
                            "mantle": "mnt-mantle",
                            "blast": "blast-blast",
                            "linea": "eth-ethereum",
                            "scroll": "eth-ethereum",
                            "zkevm": "eth-ethereum",
                            "ronin": "ron-ronin-token",
                            "cardano": "ada-cardano",
                            "tron": "trx-tron"
                            }
        self.tokens_by_id = self.get_tokens()
        self.contracts_by_chain = self.get_contracts()

    @staticmethod
    def parse_token(line):
        line = line.strip().lstrip(',').strip()

        pattern1 = r"\(?'?([\w-]+)'?,\s*'?([\w-]+)'?,\s*'?([\w-]+)'?,\s*(0x[a-fA-F0-9]+|'[\w]+'),\s*(\d+)\)?"
        pattern2 = r"\(?'?([\w-]+)'?,\s*'?([\w-]+)'?,\s*(0x[a-fA-F0-9]+|'[\w]+'),\s*(\d+)\)?"

        try:
            match1 = re.match(pattern1, line)
            if match1:
                return {
                    "id": match1.group(1),
                    "blockchain": match1.group(2),
                    "symbol": match1.group(3),
                    "contract_address": match1.group(4).lower() if match1.group(4).startswith('0x') else match1.group(
                        4).strip("'"),
                    "decimal": int(match1.group(5))
                }

            match2 = re.match(pattern2, line)
            if match2:
                return {
                    "id": match2.group(1),
                    "symbol": match2.group(2),
                    "contract_address": match2.group(3).lower() if match2.group(3).startswith('0x') else match2.group(
                        3).strip("'"),
                    "decimal": int(match2.group(4)),
                    "blockchain": None
                }
        except Exception as e:
            logging.warning(f"Failed to parse line: {line}. Error: {str(e)}")
            return None

        logging.warning(f"Failed to parse line: {line}")
        return None

    @staticmethod
    def get_tokens():
        logging.info(f"INFO: getting all the coins from coinpaprika...")
        try:
            resp = requests.get("https://api.coinpaprika.com/v1/coins")
            resp.raise_for_status()
            result = {e["id"]: e for e in resp.json()}
            logging.info(f"INFO: retrieved {len(result)} coins")
            return result
        except requests.HTTPError:
            raise

    @staticmethod
    def get_contracts_for_chain(chain_slug):
        logging.info(f"INFO: getting all the contracts from coinpaprika for chain: {chain_slug}...")
        try:
            resp = requests.get(f"https://api.coinpaprika.com/v1/contracts/{chain_slug}")
            resp.raise_for_status()
            result = {e["address"].lower(): e for e in resp.json()}
            logging.info(f"INFO: retrieved {len(result)} contracts")
            return result
        except requests.HTTPError:
            raise

    def get_contracts(self):
        return {chain: self.get_contracts_for_chain(slug) for chain, slug in self.chain_slugs.items()}

    def validate_token(self, new_line):
        token = self.parse_token(new_line)
        try:
            api_token = self.tokens_by_id[token['id']]
            logging.info(f"INFO: verifying {token['id']}")
        except KeyError:
            logging.warning(f"WARN: Line: {new_line} token_id not found in CoinPaprika API")
            raise
        # Confirm Symbol
        if token['symbol']:
            assert api_token['symbol'].lower() == token['symbol'].lower() \
                , f"ERROR: {token['id']} Provided symbol: {token['symbol']} does not match CoinPaprika source: {api_token['symbol']}"
        else:
            logging.warning(f"WARN: Line: {new_line} Symbol is None")

        # Confirm Active
        if token["id"]:
            assert api_token['is_active'] is True, f"ERROR: Token: {token['id']} is not active"
        else:
            logging.warning(f"WARN: Line: {new_line} token_id is None")

        if token['blockchain']:
            assert token['blockchain'] in self.chain_slugs \
                , f"ERROR: chain: {token['blockchain']} not supported in the price checker, could not check contract or chain"

            if token['contract_address']:
                if token['contract_address'] in self.contracts_by_chain[token['blockchain']]:
                    # Confirm Contract Listed
                    api_contract_id = self.contracts_by_chain[token['blockchain']][token['contract_address']]
                    assert token['id'] == api_contract_id['id'] \
                        , f"ERROR: ID: {token['id']} (contract address: {token['contract_address']},chain {token['blockchain']}) does not match CoinPaprika id :{api_contract_id['id']}" \
                          f" (Not uncommon! share block explorer link to confirm contract)"
                else:
                    logging.warning(
                        f"WARN: contract {token['contract_address']} for token {token['id']} not found in the "
                        f"contracts for chain: {token['blockchain']}"
                        f"(Not uncommon! share block explorer link to confirm contract)")
            else:
                logging.warning(f"WARN: Line: {new_line} contract_address is None")
        else:
            logging.warning(f"WARN: Line: {new_line} chain is None")
