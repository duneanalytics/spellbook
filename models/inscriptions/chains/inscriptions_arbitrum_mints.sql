{% set blockchain = 'arbitrum' %}

{{ config(
        
        schema = 'inscriptions_' + blockchain,
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
)
}}

{{inscriptions_mints(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
        , first_inscription_block = 102376455
)}}
-- First inscription block is 102376455, 2023-06-18 09:35: https://dune.com/queries/3253988