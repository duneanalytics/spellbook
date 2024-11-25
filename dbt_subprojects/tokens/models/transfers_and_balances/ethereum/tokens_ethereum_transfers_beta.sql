{{config(
    schema = 'tokens_ethereum',
    alias = 'transfers_beta',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude"]\') }}'
)
}}

{{
    transfers_enrich_beta(
        base_transfers = ref('tokens_ethereum_base_transfers')
        , tokens_erc20_model = source('tokens', 'erc20')
        , prices_model = ref('prices_hour')
        , evms_info_model = source('evms','info')
        , transfers_start_date = '2024-08-01'
        , transfers_end_date = '2024-08-02'
        , blockchain = 'ethereum'
    )
}}
