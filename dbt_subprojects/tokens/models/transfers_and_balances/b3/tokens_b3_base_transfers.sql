{{config(
     schema = 'tokens_b3'
     , alias = 'base_transfers'
     , partition_by = ['block_month']
     , materialized = 'incremental'
     , file_format = 'delta'
     , incremental_strategy = 'merge'
     , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
     , unique_key = ['block_date','unique_key']
 )
}}

{{transfers_base(
     blockchain='b3'
     , traces = source('b3','traces')
     , transactions = source('b3','transactions')
     , erc20_transfers = source('erc20_b3','evt_Transfer')
)
}}
