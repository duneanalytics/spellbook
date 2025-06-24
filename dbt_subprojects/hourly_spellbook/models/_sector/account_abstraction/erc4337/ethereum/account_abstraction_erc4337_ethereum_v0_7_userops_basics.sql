{{ config
(
    alias = 'v0_7_userops_basics',
    schema = 'account_abstraction_erc4337_ethereum',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'ethereum' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set version = 'v0.7' %}
{% set deployed_date = '2024-02-21' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'ethereum',
        version = 'v0.7',
        userops_evt_model = source('erc4337_ethereum','EntryPoint_v0_7_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_ethereum', 'EntryPoint_v0_7_call_handleOps')
    )
}}