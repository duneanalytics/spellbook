{% set blockchain = 'base' %}

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
        , first_inscription_block = 2041131
)}}
-- First inscription block is 2041131, 2023-08-01 06:33: https://dune.com/queries/3254019