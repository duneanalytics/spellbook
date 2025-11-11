{{ config(
    schema = 'tokens_story',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
) 
}}

-- story token on STORY
{{ transfers_base(
    blockchain='story',
    traces = source('story','traces'),
    transactions = source('story','transactions'),
    erc20_transfers = source('erc20_story','evt_Transfer')
  ) 
}}
