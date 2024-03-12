{{ config
(
    alias = 'v0_7_userops_basics',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'base' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0x4200000000000000000000000000000000000006' %}
{% set version = 'v0.7' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'base',
        version = 'v0.7',
        userops_evt_model = source('erc4337_base','EntryPoint_v0_7_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_base', 'EntryPoint_v0_7_call_handleOps')
    )
}}