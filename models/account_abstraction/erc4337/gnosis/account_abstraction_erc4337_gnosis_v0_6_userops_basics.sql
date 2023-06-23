{{ config
(
    alias ='v0_6_userops_basics',
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'gnosis' %}
{% set gas_symbol = 'DAI' %}
{% set wrapped_gas_address = '0x44fa8e6f47987339850636f88629646662444217' %}
{% set version = 'v0.6' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'gnosis',
        version = 'v0.6',
        userops_evt_model = source('erc4337_gnosis','EntryPoint_v0_6_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_gnosis', 'EntryPoint_v0_6_call_handleOps')
    )
}}