{% set blockchain = 'optimism' %}

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
        , first_inscription_block = 105741421
)}}
-- First inscription block is 105741421, 2023-06-18 09:46: https://dune.com/queries/3253991