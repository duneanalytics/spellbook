{{ config(
	tags=['legacy', 'remove'],

    alias = alias('v0_5_userops_basics', legacy_model=True),
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'ethereum' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set version = 'v0.5' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics_legacy(
        blockchain = 'ethereum',
        version = 'v0.5',
        userops_evt_model = source('erc4337_ethereum','EntryPoint_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_ethereum', 'EntryPoint_call_handleOps')
    )
}}