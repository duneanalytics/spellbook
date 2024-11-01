{{ config(
    alias = 'v0_6_userops_basics',
    schema = 'account_abstraction_erc4337_b3',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)}}

{% set chain = 'b3' %}
{% set gas_symbol = 'B3' %}
{% set wrapped_gas_address = '0x4200000000000000000000000000000000000006' %}  -- Need to update with actual wrapped gas token address
{% set version = 'v0.6' %}
{% set deployed_date = '2023-02-15' %}

{{
    erc4337_userops_basics(
        blockchain = 'b3',
        version = 'v0.6',
        userops_evt_model = source('erc4337_b3','EntryPoint_v0_6_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_b3', 'EntryPoint_v0_6_call_handleOps')
    )
}} 