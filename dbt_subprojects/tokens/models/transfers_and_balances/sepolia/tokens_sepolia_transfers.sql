{{config(
    schema = 'tokens_sepolia',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["sepolia"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude", "hildobby"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_sepolia_base_transfers')
        , tokens_erc20_sepolial = source('tokens', 'erc20')
        , prices_sepolial = source('prices', 'usd')
        , evms_info_sepolial = source('evms','info')
        , transfers_start_date = '2020-04-22'
        , blockchain = 'sepolia'
    )
}}
