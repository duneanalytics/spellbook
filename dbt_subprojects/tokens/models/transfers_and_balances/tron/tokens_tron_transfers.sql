{{config(
    schema = 'tokens_tron',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["tron"]\',
                                "sector",
                                "tokens",
                                \'["0xRob"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_tron_base_transfers')
        , transfers_start_date = '2018-10-11'
        , blockchain = 'tron'
    )
}}
