{% set blockchain = 'zora' %}

{{ config(
        
        schema = 'inscription_' + blockchain,
        alias = 'all',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash']
)
}}

{{inscription_all(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
        , first_inscription_block = 2137565
)}}
-- First inscription block is 2137565, 2023-08-02 09:36: https://dune.com/queries/3254037