{{config(
    schema = 'tokens_lens',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["lens"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_lens_base_transfers')
        , tokens_erc20_model = source('tokens', 'erc20')
        , prices_model = source('prices', 'usd')
        , transfers_start_date = '2022-05-18'
        , blockchain = 'lens'
    )
}} 