{{config(
    schema = 'tokens_bnb',
    alias = 'transfers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude"]\') }}'
)
}}
--add start date of first transfer, to add not incremental flag in macro
{{
    transfers_enrich(
        base_transfers = ref('tokens_bnb_base_transfers')
        , tokens_erc20_model = source('tokens', 'erc20')
        , prices_model = source('prices', 'usd')
        , evms_info_model = ref('evms_info')
    )
}}