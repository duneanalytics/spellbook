{{ config(
    schema = 'tokens_mezo',
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
    blockchain = 'mezo',
    traces = source('mezo','traces'),
    transactions = source('mezo','transactions'),
    erc20_transfers = source('erc20_mezo','evt_Transfer')
  ) 
}}
