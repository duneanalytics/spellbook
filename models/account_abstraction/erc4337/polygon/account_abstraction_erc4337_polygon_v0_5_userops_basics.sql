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


{% set chain = 'polygon' %}
{% set gas_symbol = 'MATIC' %}
{% set wrapped_gas_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' %}
{% set version = 'v0.5' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'polygon',
        version = 'v0.5',
        userops_evt_model = source('erc4337_polygon','EntryPoint_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_polygon', 'EntryPoint_call_handleOps')
    )
}}