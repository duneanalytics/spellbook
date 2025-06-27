{{config(
     schema = 'tokens_shape'
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
     blockchain='shape'
     , traces = source('shape','traces')
     , transactions = source('shape','transactions')
     , erc20_transfers = source('erc20_shape','evt_Transfer')
)
}}
