{{config(
    schema = 'tokens_polygon',
    alias = 'transfers',
    materialized = 'view',
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_polygon_base_transfers')
        , tokens_erc20_model = source('tokens', 'erc20')
        , prices_model = source('prices', 'usd')
        , evms_info_model = source('evms','info')
        , transfers_start_date = '2020-05-30'
        , blockchain = 'polygon'
    )
}}
