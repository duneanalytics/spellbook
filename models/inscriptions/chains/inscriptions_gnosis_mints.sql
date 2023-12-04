{% set blockchain = 'gnosis' %}

{{ config(
        
        schema = 'inscriptions_' + blockchain,
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
)
}}

{{inscriptions_mints(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
        , first_inscription_block = 28732751
)}}
-- First inscription block is 28732751, 2023-07-01 16:09: https://dune.com/queries/3253998