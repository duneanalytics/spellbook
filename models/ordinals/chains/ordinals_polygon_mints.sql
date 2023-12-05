{% set blockchain = 'polygon' %}

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
        , first_ordinal_block = 44047666
)}}
-- First ordinal block is 44047666, 2023-06-18 10:05: https://dune.com/queries/3253953
