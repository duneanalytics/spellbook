{{ config(
    schema = 'balances_ethereum_eth',
    tags=['dunesql'],
    alias = alias('eth_suicide'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'address']
    )
}}

{{
    balances_fungible_suicide(
        blockchain = 'ethereum'
    )
}}
