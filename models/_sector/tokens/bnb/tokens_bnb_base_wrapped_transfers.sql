{{config(
    tags = ['base_transfers_macro'],
    schema = 'tokens_bnb',
    alias = 'base_wrapped_transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['unique_key'],
)
}}

SELECT *
FROM
(
    {{transfers_base_wrapped_token(
        blockchain='bnb',
        transactions = source('bnb','transactions'),
        wrapped_token_deposit = source('bnb_bnb', 'WBNB_evt_Deposit'),
        wrapped_token_withdrawal = source('bnb_bnb', 'WBNB_evt_Withdrawal')
    )
    }}
)
