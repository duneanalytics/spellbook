{{ config
(
    alias = 'v0_8_account_deployed',
    schema = 'account_abstraction_erc4337_base',

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
        version = 'v0.8',
        account_deployed_evt_model = source('erc4337_base','entrypoint_evt_accountdeployed'),
    )
}}