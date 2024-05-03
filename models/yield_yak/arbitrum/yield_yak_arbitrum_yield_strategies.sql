{{
    config(
        schema = 'yield_yak_arbitrum',
        alias = 'yield_strategies',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address']
    )
}}

{{
    yield_yak_yield_strategies(
        blockchain = 'arbitrum'
    )
}}
