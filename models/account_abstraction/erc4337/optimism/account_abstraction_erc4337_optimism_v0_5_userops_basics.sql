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


{% set chain = 'optimism' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0x4200000000000000000000000000000000000006' %}
{% set version = 'v0.5' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'optimism',
        version = 'v0.5',
        userops_evt_model = source('erc4337_optimism','EntryPoint_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_optimism', 'EntryPoint_call_handleOps')
    )
}}