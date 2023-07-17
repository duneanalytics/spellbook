{{ config
(
    alias = alias('v0_6_userops_basics'),
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "erc4337",
                                    \'["0xbitfly", "hosuke"]\') }}'
)
}}


{% set chain = 'ethereum' %}
{% set gas_symbol = 'ETH' %}
{% set wrapped_gas_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set version = 'v0.6' %}
{% set deployed_date = '2023-02-15' %}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'ethereum',
        version = 'v0.6',
        userops_evt_model = source('erc4337_ethereum','EntryPoint_v0_6_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_ethereum', 'EntryPoint_v0_6_call_handleOps')
    )
}}