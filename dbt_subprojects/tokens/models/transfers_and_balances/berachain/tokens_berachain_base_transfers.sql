{{ config(
    schema = 'tokens_berachain',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
) 
}}

-- BERA token on Berachain
{{ transfers_base(
    blockchain='berachain',
    traces = source('berachain','traces'),
    transactions = source('berachain','transactions'),
    erc20_transfers = source('erc20_berachain','evt_Transfer')
  ) 
}}
