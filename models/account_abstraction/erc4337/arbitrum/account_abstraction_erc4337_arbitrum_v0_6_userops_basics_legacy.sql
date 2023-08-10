{{ config(
	tags=['legacy'],
	
    alias = alias('v0_6_userops_basics', legacy_model=True),
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'arbitrum' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' %}
{% set version = 'v0.6' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics_legacy(
        blockchain = 'arbitrum',
        version = 'v0.6',
        userops_evt_model = source('erc4337_arbitrum','EntryPoint_v0_6_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_arbitrum', 'EntryPoint_v0_6_call_handleOps')
    )
}}