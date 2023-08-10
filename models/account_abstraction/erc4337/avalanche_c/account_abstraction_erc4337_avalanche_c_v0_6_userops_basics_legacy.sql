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


{% set chain = 'avalanche_c' %}
{% set gas_symbol = 'AVAX' %}
{% set wrapped_gas_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' %}
{% set version = 'v0.6' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics_legacy(
        blockchain = 'avalanche_c',
        version = 'v0.6',
        userops_evt_model = source('erc4337_avalanche_c','EntryPoint_v0_6_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_avalanche_c', 'EntryPoint_v0_6_call_handleOps')
    )
}}