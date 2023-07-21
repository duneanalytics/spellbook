{{ config
(
    alias = alias('v0_5_userops_basics'),
    tags=['dunesql'],
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'arbitrum' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' %}
{% set version = 'v0.5' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'arbitrum',
        version = 'v0.5',
        userops_evt_model = source('erc4337_arbitrum','EntryPoint_v0_5_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_arbitrum', 'EntryPoint_v0_5_call_handleOps')
    )
}}