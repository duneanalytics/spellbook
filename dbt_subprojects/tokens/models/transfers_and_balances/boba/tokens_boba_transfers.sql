{{config(
     schema = 'tokens_boba',
     alias = 'transfers',
     partition_by = ['block_month'],
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['block_date','unique_key'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
     post_hook='{{ expose_spells(blockchains = \'["boba"]\',
                                spell_type = "sector",
                                spell_name = "tokens",
                                contributors = \'["hosuke"]\') }}'
 )
}}

{{
     transfers_enrich(
         base_transfers = ref('tokens_boba_base_transfers')
         , transfers_start_date = '2021-10-28'
         , blockchain = 'boba'
     )
}} 