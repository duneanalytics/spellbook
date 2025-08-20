{{ config(
    schema = 'tokens_sophon',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
) 
}}

-- SOPH token on ZK Stack chain
{{ transfers_base(
    blockchain='sophon',
    traces = source('sophon','traces'),
    transactions = source('sophon','transactions'),
    erc20_transfers = source('erc20_sophon','evt_Transfer'),
    include_traces = false
  ) 
}}
