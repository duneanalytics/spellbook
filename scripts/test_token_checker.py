import pytest
from requests import HTTPError

from token_checker import TokenChecker


def test_test_token_checker_attrs1():
    with pytest.raises(HTTPError):
        test_token_checker = TokenChecker(new_line='("test", null, "TEST", null, null),')

def test_test_token_checker_attrs2():
    test_token_checker = TokenChecker(new_line='("0xbtc-0xbitcoin","arbitrum","0xBTC","0x7cb16cb78ea464ad35c8a50abf95dff3c9e09d5d",8),')
    assert "0xbtc-0xbitcoin" == test_token_checker.token_id
    assert "arbitrum" == test_token_checker.blockchain
    assert "0xBTC" == test_token_checker.symbol
    assert "0x7cb16cb78ea464ad35c8a50abf95dff3c9e09d5d" == test_token_checker.contract_address

def test_token_missing_contract1():
    test_token_checker = TokenChecker(new_line='("dai-dai", "avalanche_c", "DAI", "0xd586e7f844cea2f87f50152665bcbc2c279d8d70", 18),')
    with pytest.raises(AssertionError):
        test_token_checker.validate_token()

def test_token_missing_contract2():
    test_token_checker = TokenChecker(new_line='("link-chainlink","arbitrum","LINK","0xf97f4df75117a78c1a5a0dbb814af92458539fb4",18),')
    with pytest.raises(AssertionError):
        test_token_checker.validate_token()

def test_valid_token1():
    test_token_checker = TokenChecker(new_line='("ape-apecoin", "ethereum", "APE", "0x4d224452801aced8b2f0aebe155379bb5d594381", 18),')
    test_token_checker.validate_token()

def test_valid_token2():
    test_token_checker = TokenChecker(new_line='("1inch-1inch", "ethereum", "1INCH", "0x111111111117dc0aa78b770fa6a738034120c302", 18),', )
    test_token_checker.validate_token()

def test_valid_token3():
    test_token_checker = TokenChecker(new_line='("usdt-tether", "bnb", "USDT", "0x55d398326f99059ff775485246999027b3197955", 18),')
    test_token_checker.validate_token()

def test_valid_token4():
    test_token_checker = TokenChecker(new_line='("sps-splintershards", "bnb", "SPS", "0x1633b7157e7638c4d6593436111bf125ee74703f", 18),')
    test_token_checker.validate_token()

def test_valid_token5():
    test_token_checker = TokenChecker(new_line='("dfl-defi-land", "solana", "DFL", "DFL1zNkaGPWm1BqAVqRjCZvHmwTFrEaJtbzJWgseoNJh", 9),')
    test_token_checker.validate_token()

def test_valid_token6():
    test_token_checker = TokenChecker(new_line='("bets-betswirl", "polygon", "BETS", "0x9246a5f10a79a5a939b0c2a75a3ad196aafdb43b", 18),')
    test_token_checker.validate_token()