{{
    config(
        schema = 'dodo_bnb',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set config_markets %}
    WITH dodo_view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS 
    (
        VALUES
        (0x327134dE48fcDD75320f4c32498D1980470249ae, 'WBNB', 'BUSD', 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
        (0x5BDCf4962FDED6B7156E710400F4C4c031f600dC, 'KOGE', 'WBNB', 0xe6DF05CE8C8301223373CF5B969AFCb1498c5528, 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c),
        (0xBe60d4c4250438344bEC816Ec2deC99925dEb4c7, 'BUSD', 'USDT', 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56, 0x55d398326f99059fF775485246999027B3197955),
        (0xC64a1d5C819B3c9113cE3DB32B66D5D2b05B4CEf, 'BTCB', 'BUSD', 0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
        (0x89E5015ff12E4536691aBfe5f115B1cB37a35465, 'ETH', 'BUSD', 0x2170Ed0880ac9A755fd29B2688956BD959F933F8, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
        (0x6064DBD0fF10BFeD5a797807042e9f63F18Cfe10, 'USDC', 'BUSD', 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
        (0xb1327B6402ddbA34584Ab59fbe8Ac7cbF43f6353, 'DOT', 'BUSD', 0x7083609fCE4d1d8Dc0C979AAb8c869Ea2C873402,0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
        (0x8d078451a63D118bACC9Cc46698cc416f81C93E2, 'LINK', 'BUSD', 0xF8A0BF9cF54Bb92F17374d9e9A321E6a111a51bD, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
        (0x82AfF931d74F0645Ce80e8f419b94c8F93952686, 'WBNB', 'USDT', 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c, 0x55d398326f99059fF775485246999027B3197955)
    )
    SELECT * FROM dodo_view_markets
{% endset %}

{%
    set config_other_sources = [
        {'version': '2_dvm', 'source': 'DVM_evt_DODOSwap'},
        {'version': '2_dpp', 'source': 'DPP_evt_DODOSwap'},
        {'version': '2_dpp', 'source': 'DPPAdvanced_evt_DODOSwap'},
        {'version': '2_dpp', 'source': 'DPPOracle_evt_DODOSwap'},
        {'version': '2_dsp', 'source': 'DSP_evt_DODOSwap'},
    ]
%}

{{
    dodo_compatible_trades(
        blockchain = 'bnb',
        project = 'dodo',
        markets = config_markets,
        decoded_project = 'dodoex',
        sell_base_token_source = 'DODO_evt_SellBaseToken',
        buy_base_token_source = 'DODO_evt_BuyBaseToken',
        other_sources = config_other_sources
    )
}}
