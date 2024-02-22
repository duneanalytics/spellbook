{{config(
    schema = 'tokens_polygon',
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
    blockchain='polygon',
    traces = source('polygon','traces'),
    transactions = source('polygon','transactions'),
    erc20_transfers = source('erc20_polygon','evt_transfer'),
    native_contract_address = null
)
}}

UNION ALL

SELECT *
FROM
(
    {{transfers_base_wrapped_token(
        blockchain='polygon',
        transactions = source('polygon','transactions'),
        wrapped_token_deposit = source('mahadao_polygon', 'wmatic_evt_deposit'),
        wrapped_token_withdrawal = source('mahadao_polygon', 'wmatic_evt_withdrawal')
    )
    }}
)
