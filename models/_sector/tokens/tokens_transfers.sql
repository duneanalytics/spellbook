{{config(
    schema = 'tokens',
    alias = 'transfers',
    partition_by = ['blockchain', 'token_standard', 'block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'unique_key'],
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "optimism", "polygon", "zksync", "zora"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_base_transfers')
        , tokens_erc20_model = ref('tokens_erc20')
        , prices_model = source('prices', 'usd')
        , evms_info_model = ref('evms_info')
    )
}}