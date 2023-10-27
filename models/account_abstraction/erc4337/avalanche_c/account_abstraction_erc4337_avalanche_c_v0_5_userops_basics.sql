{{ config(
    alias = 'v0_5_userops_basics',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'avalanche_c' %}
{% set gas_symbol = 'AVAX' %}
{% set wrapped_gas_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' %}
{% set version = 'v0.5' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'avalanche_c',
        version = 'v0.5',
        userops_evt_model = source('erc4337_avalanche_c','EntryPoint_v0_5_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_avalanche_c', 'EntryPoint_v0_5_call_handleOps')
    )
}}