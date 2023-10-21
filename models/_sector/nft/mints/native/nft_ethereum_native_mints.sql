{{
    config(
        tags = ['dunesql'],
        schema = 'nft_ethereum',
        alias = alias('native_mints'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items','currency_contract']
    )
}}

{% set blockchain = 'ethereum' %}

{{
    nft_mints(
        blockchain = blockchain,
        src_contracts = source(blockchain, 'contracts'),
        src_traces = source(blockchain, 'traces'),
        src_transactions = source(blockchain, 'transactions'),
        src_erc20_evt_transfer = source('erc20_' ~ blockchain, 'evt_transfer'),
        nft_transfers = ref('nft_' ~ blockchain ~ '_transfers'),
        nft_aggregators = ref('nft_' ~ blockchain ~ '_aggregators'),
        tokens_nft = ref('tokens_' ~ blockchain ~ '_nft'),
        default_currency_symbol = 'ETH',
        default_currency_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        addresses_defi = ref('addresses_' ~ blockchain ~ '_defi')
    )
}}
