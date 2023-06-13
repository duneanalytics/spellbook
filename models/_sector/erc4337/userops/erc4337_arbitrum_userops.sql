{{ config(
    schema ='erc4337_arbitrum',
    alias = 'userops',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke]\') }}'
)}}

-- min deployed_date on chain
{% set deployed_date = '2023-02-15' %}

{% set erc4337_base_models = [
    ref('erc4337_v0_5_arbitrum_userops_basics')
    , ref('erc4337_v0_6_arbitrum_userops_basics')
] %}

{{
    erc4337_userops_enrichments(
        blockchain = 'arbitrum',
        base_models = erc4337_base_models,
        wrapped_gas_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
        gas_symbol = 'ETH',
        deployed_date = '2023-02-15'
        transactions_model = {{ source('arbitrum', 'transactions') }},
        prices_model = {{ source('prices','usd') }}
    )
}}