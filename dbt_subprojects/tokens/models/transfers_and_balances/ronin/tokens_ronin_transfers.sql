{{config(
    schema = 'tokens_ronin',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["ronin"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude", "peterrliem"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_ronin_base_transfers')
        , transfers_start_date = '2021-01-24'
        , blockchain = 'ronin'
    )
}}
