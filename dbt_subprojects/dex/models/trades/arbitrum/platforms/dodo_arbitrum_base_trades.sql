{{
    config(
        schema = 'dodo_arbitrum',
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
        (0xFE176A2b1e1F67250d2903B8d25f56C0DaBcd6b2, 'WETH', 'USDC', 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1, 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8),
        (0xe4B2Dfc82977dd2DCE7E8d37895a6A8F50CbB4fB, 'USDT', 'USDC', 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9, 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8),
        (0xb42a054D950daFD872808B3c839Fbb7AFb86E14C, 'WBTC', 'USDC', 0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f, 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8)
    )
    SELECT * FROM dodo_view_markets
{% endset %}

{%
    set config_other_sources = [
        {'version': '2_dvm', 'source': 'DVM_evt_DODOSwap'},
        {'version': '2_dpp', 'source': 'DPPOracle_evt_DODOSwap'},
        {'version': '2_dsp', 'source': 'DSP_evt_DODOSwap'},
    ]
%}

{{
    dodo_compatible_trades(
        blockchain = 'arbitrum',
        project = 'dodo',
        markets = config_markets,
        sell_base_token_source = 'DODO_evt_SellBaseToken',
        buy_base_token_source = 'DODO_evt_BuyBaseToken',
        other_sources = config_other_sources
    )
}}
