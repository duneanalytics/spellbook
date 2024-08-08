{{ config(
    schema = 'tokens_ethereum',
    alias = 'base_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
    )
}}

{{transfers_base(
    blockchain='ethereum',
    traces = source('ethereum','traces'),
    transactions = source('ethereum','transactions'),
    erc20_transfers = source('erc20_ethereum','evt_Transfer'),
    native_contract_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
)}}

UNION ALL

SELECT *
FROM
(
    {{transfers_base_wrapped_token(
        blockchain='ethereum',
        transactions = source('ethereum','transactions'),
        wrapped_token_deposit = source('zeroex_ethereum', 'weth9_evt_deposit'),
        wrapped_token_withdrawal = source('zeroex_ethereum', 'weth9_evt_withdrawal'),
    )
    }}
)
