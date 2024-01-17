{% set blockchain = 'ethereum' %}

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
        , first_inscription_block = 17498959
)}}
-- First inscription block is 17498959, 2023-06-17 10:33: https://dune.com/queries/3254006