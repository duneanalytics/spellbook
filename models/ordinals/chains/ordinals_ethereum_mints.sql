{% set blockchain = 'ethereum' %}

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
        , first_ordinal_block = 17498959
)}}
-- First ordinal block is 17498959, 2023-06-17 10:33: https://dune.com/queries/3254006