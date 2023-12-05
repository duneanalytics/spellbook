{% set blockchain = 'bnb' %}

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
        , first_ordinal_block = 29257005
)}}
-- First ordinal block is 29257005, 2023-06-20 04:37: https://dune.com/queries/3254013