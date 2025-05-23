{{config(
    schema = 'tokens_gnosis',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

{{transfers_base(
    blockchain='gnosis',
    traces = source('gnosis','traces'),
    transactions = source('gnosis','transactions'),
    erc20_transfers = source('erc20_gnosis','evt_Transfer')
)
}}

UNION ALL

SELECT *
FROM
(
    {{transfers_base_wrapped_token(
        blockchain='gnosis',
        transactions = source('gnosis','transactions'),
        wrapped_token_deposit = source('wxdai_gnosis', 'WXDAI_evt_Deposit'),
        wrapped_token_withdrawal = source('wxdai_gnosis', 'WXDAI_evt_Withdrawal'),
    )
    }}
)