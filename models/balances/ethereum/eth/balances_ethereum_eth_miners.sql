{{ config(
    schema = 'balances_ethereum',
    tags=['dunesql'],
    alias = alias('eth_miners'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'miner']
    )
}}

{{
    balances_fungible_miners(
        blockchain = 'ethereum'
    )
}}
