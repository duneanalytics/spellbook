{{ config
(
    alias = 'v0_7_userops_basics',
    schema = 'account_abstraction_erc4337_optimism',
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
{% set version = 'v0.7' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'optimism',
        version = 'v0.7',
        userops_evt_model = source('erc4337_optimism','entrypoint_v0_7_evt_useroperationevent'),
        handleops_call_model = source('erc4337_optimism', 'entrypoint_v0_7_call_handleops')
    )
}}