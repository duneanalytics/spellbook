{{config(
    schema = 'tokens_ink'
    , alias = 'transfers'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date','unique_key']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , post_hook='{{ expose_spells(blockchains = \'["ink"]\',
                                spell_type = "sector",
                                spell_name = "tokens",
                                contributors = \'["hosuke"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_ink_base_transfers')
        , tokens_erc20_model = source('tokens', 'erc20')
        , prices_model = source('prices', 'usd')
        , transfers_start_date = '2024-12-06'
        , blockchain = 'ink'
    )
}} 