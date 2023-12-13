{% set blockchain = 'bnb' %}

{{ config(
        
        schema = 'dex_' + blockchain,
        alias = 'sandwiched',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{{dex_sandwiched(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
        , sandwiches = ref('dex_' + blockchain + '_sandwiches')
)}}
