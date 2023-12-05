{% set blockchain = 'avalanche_c' %}

{{ config(
        
        schema = 'ordinals_' + blockchain,
        alias = 'sandwiches',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{ordinal_mints(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
        , first_ordinal_block = 31918263
)}}
-- First ordinal block is 31918263, 2023-06-28 16:17: https://dune.com/queries/3254018