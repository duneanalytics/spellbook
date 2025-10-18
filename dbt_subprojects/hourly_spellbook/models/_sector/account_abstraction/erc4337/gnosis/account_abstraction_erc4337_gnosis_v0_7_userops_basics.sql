{{ config
(
    alias = 'v0_7_userops_basics',
    schema = 'account_abstraction_erc4337_gnosis',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


{% set chain = 'gnosis' %}
{% set gas_symbol = 'DAI' %}
{% set wrapped_gas_address = '0x44fa8e6f47987339850636f88629646662444217' %}
{% set version = 'v0.7' %}
{% set deployed_date = '2024-04-02' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'gnosis',
        version = 'v0.7',
        userops_evt_model = source('erc4337_gnosis','entrypoint_v0_7_evt_useroperationevent'),
        handleops_call_model = source('erc4337_gnosis', 'entrypoint_v0_7_call_handleops')
    )
}}