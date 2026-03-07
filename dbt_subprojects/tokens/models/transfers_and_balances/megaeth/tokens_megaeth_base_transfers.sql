{{ config(
    schema = 'tokens_megaeth',
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
    blockchain = 'megaeth',
    traces = source('megaeth','traces'),
    transactions = source('megaeth','transactions'),
    erc20_transfers = source('erc20_megaeth','evt_Transfer')
  ) 
}}
