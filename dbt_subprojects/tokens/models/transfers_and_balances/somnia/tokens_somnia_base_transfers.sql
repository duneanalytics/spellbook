{{ config(
    schema = 'tokens_somnia',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
) 
}}

-- somnia token on SOMNIA
{{ transfers_base(
    blockchain='somnia',
    traces = source('somnia','traces'),
    transactions = source('somnia','transactions'),
    erc20_transfers = source('erc20_somnia','evt_Transfer')
  ) 
}}
