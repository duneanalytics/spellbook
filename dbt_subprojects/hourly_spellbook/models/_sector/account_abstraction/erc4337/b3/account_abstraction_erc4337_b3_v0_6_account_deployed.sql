{{ config(
    alias = 'v0_6_account_deployed',
    schema = 'account_abstraction_erc4337_b3',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['userop_hash', 'tx_hash']
)}}

{{
    erc4337_account_deployed(
        blockchain = 'b3',
        version = 'v0.6',
        account_deployed_evt_model = source('erc4337_b3','EntryPoint_v0_6_evt_AccountDeployed')
    )
}} 