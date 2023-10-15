{{
    config(
        tags = ['dunesql'],
        schema = 'nft_polygon',
        alias = alias('native_mints'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
    )
}}

{% set blockchain = 'polygon' %}

{{
    nft_mints(
        blockchain = blockchain,
        src_contracts = source(blockchain, 'contracts'),
        src_traces = source(blockchain, 'traces'),
        src_transactions = source(blockchain, 'transactions'),
        src_prices_usd = source('prices', 'usd'),
        src_erc20_evt_transfer = source('erc20_' ~ blockchain, 'evt_transfer'),
        nft_transfers = ref('nft_' ~ blockchain ~ '_transfers'),
        nft_aggregators = ref('nft_' ~ blockchain ~ '_aggregators'),
        tokens_nft = ref('tokens_' ~ blockchain ~ '_nft'),
        default_currency_symbol = 'MATIC',
        default_currency_contract = '0xCC42724C6683B7E57334c4E856f4c9965ED682bD'
    )
}}
