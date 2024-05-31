{{
    config(
        schema = 'account_abstraction_erc4337_celo',
        alias = 'v0_6_account_deployed',
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
        blockchain = 'celo',
        version = 'v0.6',
        account_deployed_evt_model = source('erc4337_celo', 'EntryPoint_evt_AccountDeployed'),
    )
}}
