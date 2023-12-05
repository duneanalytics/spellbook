{% set blockchain = 'celo' %}

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
)}}
