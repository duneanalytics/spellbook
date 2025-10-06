{{ config(
    schema = 'tokens_plasma',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
) 
}}

-- plasma token on PLASMA
{{ transfers_base(
    blockchain='plasma',
    traces = source('plasma','traces'),
    transactions = source('plasma','transactions'),
    erc20_transfers = source('erc20_plasma','evt_Transfer')
  ) 
}} 