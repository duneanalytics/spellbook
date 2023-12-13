{% set blockchain = 'avalanche_c' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'crossdomain_arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{{dex_crossdomain_arbitrages(
        blockchain=blockchain
        , blocks = source(blockchain,'blocks')
        , traces = source(blockchain,'traces')
        , transactions = source(blockchain,'transactions')
        , erc20_transfers = source('erc20_' + blockchain,'evt_transfer')
        , dex_sandwiches = ref('dex_' + blockchain + '_sandwiches')
)}}
