{{ config
(tags = ['dunesql'],
    alias = alias('v0_5_userops_basics'),
    partition_by = ['block_time'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}

-- macros/models/sector/erc4337
{{
    erc4337_userops_basics(
        blockchain = 'arbitrum',
        version = 'v0.5',
        userops_evt_model = source('erc4337_arbitrum','EntryPoint_v0_5_evt_UserOperationEvent'),
        handleops_call_model = source('erc4337_arbitrum', 'EntryPoint_v0_5_call_handleOps')
    )
}}