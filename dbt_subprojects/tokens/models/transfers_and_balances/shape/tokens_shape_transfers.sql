{{config(
     schema = 'tokens_shape'
     , alias = 'transfers'
     , partition_by = ['block_month']
     , materialized = 'table'
     , tags = ['static']
     , file_format = 'delta'
     , post_hook='{{ hide_spells() }}'
     )
}}

{{
     transfers_enrich(
         base_transfers = ref('tokens_shape_base_transfers')
         , transfers_start_date = '2024-07-23'
         , blockchain = 'shape'
     )
}}
