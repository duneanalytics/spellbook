{{ config
(
    alias = 'v0_7_account_deployed',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)
}}


-- macros/models/sector/erc4337
{{
    erc4337_account_deployed(
        blockchain = 'base',
        version = 'v0.7',
        account_deployed_evt_model = source('erc4337_base','EntryPoint_v0_7_evt_AccountDeployed'),
    )
}}