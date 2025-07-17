{{config(
     schema = 'tokens_taiko'
     , alias = 'base_transfers'
     , partition_by = ['block_month']
     , materialized = 'incremental'
     , file_format = 'delta'
     , incremental_strategy = 'merge'
     , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
     , unique_key = ['block_date','unique_key']
 )
}}

-- TESTING: Add hardcoded date filter for faster testing
-- Remove this filter after testing is complete
{% set test_start_date = '2025-07-10' %} 

{{transfers_base(
     blockchain='taiko'
     , traces = source('taiko','traces')
     , transactions = source('taiko','transactions')
     , erc20_transfers = source('erc20_taiko','evt_Transfer')
)
}}
