{% set blockchain = 'goerli' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'all',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscription_all(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
        , first_inscription_block = 9199040
)}}
-- First inscription block is 9199040, 2023-06-18 11:50: https://dune.com/queries/3291782