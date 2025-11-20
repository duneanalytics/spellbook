{{ config(
    schema = 'tokens_henesys',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
) 
}}

{{ transfers_base(
    blockchain = 'henesys',
    traces = source('henesys','traces'),
    transactions = source('henesys','transactions'),
    erc20_transfers = source('erc20_henesys','evt_Transfer')
  ) 
}}
