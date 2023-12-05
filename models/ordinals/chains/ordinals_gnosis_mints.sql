{% set blockchain = 'gnosis' %}

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
        , first_ordinal_block = 28732751
)}}
-- First ordinal block is 28732751, 2023-07-01 16:09: https://dune.com/queries/3253998