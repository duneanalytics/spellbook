{{
    config(
        schema = 'dodo_polygon',
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
        (0x813fddeccd0401c4fa73b092b074802440544e52, 'USDC', 'USDT', 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174, 0xc2132D05D31c914a87C6611C10748AEb04B58e8F)
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
        blockchain = 'polygon',
        project = 'dodo',
        markets = config_markets,
        decoded_project = 'dodoex',
        sell_base_token_source = 'DODO_evt_SellBaseToken',
        buy_base_token_source = 'DODO_evt_BuyBaseToken',
        other_sources = config_other_sources
    )
}}
