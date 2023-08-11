{{ config
(
    alias = alias('v0_6_userops_basics'),
    tags=['dunesql'],
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'bnb',
        version = 'v0.6',
        userops_evt_model = source('erc4337_bnb','EntryPoint_v0_6_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_bnb', 'EntryPoint_v0_6_call_handleOps')
    )
}}