{{ config(
    schema = 'tokens_tac',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
) 
}}

-- tac token on TAC
{{ transfers_base(
    blockchain='tac',
    traces = source('tac','traces'),
    transactions = source('tac','transactions'),
    erc20_transfers = source('erc20_tac','evt_Transfer')
  ) 
}}
