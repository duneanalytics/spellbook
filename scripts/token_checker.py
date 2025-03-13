import json
import logging
import re
import time
from typing import Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class TokenChecker:
    """
    TokenChecker uses Coinpaprika API to verify tokens.
    Includes improved error handling and support for new tokens.

    Attributes:
        chain_slugs (Dict[str, str]): Mapping of networks to their Coinpaprika identifiers
        session (requests.Session): HTTP session with configured retries
        tokens_by_id (Dict): Cache of all tokens from Coinpaprika
        contracts_by_chain (Dict): Cache of contracts by network
        token_details_cache (Dict): Cache of detailed token information
    """

    def __init__(self):
        """Initializes TokenChecker and loads necessary data from API."""
        self.chain_slugs = {
            "ethereum": "eth-ethereum",
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
            "tron": "trx-tron",
            "boba": "eth-ethereum",
            "shape": "eth-ethereum",
            "berachain": "bera-berachain"
        }
        
        self.session = self._create_retry_session()
        self.tokens_by_id = self.get_tokens()
        self.contracts_by_chain = self.get_contracts()
        self.token_details_cache = {}
        
    def _create_retry_session(self) -> requests.Session:
        """Creates an HTTP session with configured retries.

        Returns:
            requests.Session: Configured HTTP session
        """
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    @staticmethod
    def parse_token(line: str) -> Optional[Dict]:
        """Parses a line containing token information.

        Args:
            line (str): Line to parse

        Returns:
            Optional[Dict]: Dictionary with token information or None on error
        """
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
                    "contract_address": match1.group(4).lower() if match1.group(4).startswith('0x') else match1.group(4).strip("'"),
                    "decimal": int(match1.group(5))
                }

            match2 = re.match(pattern2, line)
            if match2:
                return {
                    "id": match2.group(1),
                    "symbol": match2.group(2),
                    "contract_address": match2.group(3).lower() if match2.group(3).startswith('0x') else match2.group(3).strip("'"),
                    "decimal": int(match2.group(4)),
                    "blockchain": None
                }
        except Exception as e:
            logging.warning(f"Failed to parse line: {line}. Error: {str(e)}")
            return None

        logging.warning(f"Failed to parse line: {line}")
        return None

    def get_tokens(self) -> Dict:
        """Gets list of all tokens from Coinpaprika.

        Returns:
            Dict: Dictionary of tokens {id: token_info}
        """
        logging.info("Getting all coins from Coinpaprika...")
        try:
            resp = self.session.get("https://api.coinpaprika.com/v1/coins")
            resp.raise_for_status()
            result = {e["id"]: e for e in resp.json()}
            logging.info(f"Retrieved {len(result)} coins")
            return result
        except requests.HTTPError as e:
            logging.error(f"Failed to get tokens: {str(e)}")
            return {}

    def get_token_details(self, token_id: str) -> Optional[Dict]:
        """Gets detailed token information, including all contracts.

        Args:
            token_id (str): Token ID in Coinpaprika

        Returns:
            Optional[Dict]: Detailed token information or None on error
        """
        if token_id in self.token_details_cache:
            return self.token_details_cache[token_id]
            
        try:
            resp = self.session.get(f"https://api.coinpaprika.com/v1/coins/{token_id}")
            resp.raise_for_status()
            details = resp.json()
            self.token_details_cache[token_id] = details
            return details
        except requests.HTTPError as e:
            logging.error(f"Failed to get token details for {token_id}: {str(e)}")
            return None

    def get_contracts_for_chain(self, chain_slug: str) -> Dict:
        """Gets list of contracts for a specific network.

        Args:
            chain_slug (str): Network identifier in Coinpaprika

        Returns:
            Dict: Dictionary of contracts {address: contract_info}
        """
        logging.info(f"Getting contracts for chain: {chain_slug}...")
        try:
            resp = self.session.get(f"https://api.coinpaprika.com/v1/contracts/{chain_slug}")
            resp.raise_for_status()
            result = {e["address"].lower(): e for e in resp.json()}
            logging.info(f"Retrieved {len(result)} contracts")
            return result
        except requests.HTTPError as e:
            logging.error(f"Failed to get contracts for chain {chain_slug}: {str(e)}")
            return {}

    def get_contracts(self) -> Dict:
        """Gets list of contracts for all supported networks.

        Returns:
            Dict: Dictionary of contracts by network {chain: {address: contract_info}}
        """
        return {chain: self.get_contracts_for_chain(slug) for chain, slug in self.chain_slugs.items()}

    def verify_token_on_chain(self, token_id: str, chain: str, contract_address: str) -> Tuple[bool, str]:
        """Verifies token on a specific network.

        Args:
            token_id (str): Token ID in Coinpaprika
            chain (str): Network name
            contract_address (str): Contract address

        Returns:
            Tuple[bool, str]: (success, message)
        """
        details = self.get_token_details(token_id)
        if not details:
            return False, "Token details not found"

        # Check all token contracts
        if "contracts" in details:
            for contract in details["contracts"]:
                if (contract.get("platform") == chain and 
                    contract.get("address", "").lower() == contract_address.lower()):
                    return True, "Contract verified"

        return False, "Contract not found in token details"

    def validate_token(self, new_line: str) -> None:
        """Validates token against Coinpaprika data.

        Args:
            new_line (str): Line containing token information to verify

        Raises:
            ValueError: If line cannot be parsed or network is not supported
        """
        token = self.parse_token(new_line)
        if not token:
            raise ValueError(f"Could not parse token from line: {new_line}")

        logging.info(f"Verifying token {token['id']}")

        # Check token existence
        token_exists = False
        if token['id'] in self.tokens_by_id:
            token_exists = True
        else:
            # Try to get detailed token information
            details = self.get_token_details(token['id'])
            if details:
                token_exists = True
            else:
                logging.warning(
                    f"Token {token['id']} (symbol: {token['symbol']}) not found in Coinpaprika. "
                    "This might be a new token - please verify manually using:\n"
                    f"1. Block explorer for chain {token['blockchain']}\n"
                    f"2. Official token contract at {token['contract_address']}\n"
                    "3. Project's official documentation or announcements"
                )

        # Check blockchain support
        if token['blockchain']:
            if token['blockchain'] not in self.chain_slugs:
                logging.warning(
                    f"Chain {token['blockchain']} not found in supported networks. "
                    "This might be a new chain - please verify manually."
                )
                return

            # Check contract if specified
            if token['contract_address']:
                if token_exists:
                    success, message = self.verify_token_on_chain(
                        token['id'], 
                        token['blockchain'], 
                        token['contract_address']
                    )
                    
                    if not success:
                        logging.warning(
                            f"Contract {token['contract_address']} for token {token['id']} "
                            f"on chain {token['blockchain']}: {message}. "
                            "Please verify manually using block explorer."
                        )
                else:
                    logging.info(
                        f"Skipping contract verification for {token['contract_address']} "
                        "as token is not yet in Coinpaprika database."
                    )
