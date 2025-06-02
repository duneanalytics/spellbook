{{config(
    schema = 'tokens_kaia'
    , alias = 'base_transfers'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , unique_key = ['block_date','unique_key']
)
}}

{{transfers_base(
    blockchain='kaia'
    , traces = source('kaia','traces')
    , transactions = source('kaia','transactions')
    , erc20_transfers = source('erc20_kaia','evt_Transfer')
)
}} 