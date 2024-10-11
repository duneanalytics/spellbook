{{config(
    schema = 'tokens_goerli',
    alias = 'transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(\'["goerli"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude", "hildobby"]\') }}'
)
}}

{{
    transfers_enrich(
        base_transfers = ref('tokens_goerli_base_transfers')
        , tokens_erc20_goerlil = source('tokens', 'erc20')
        , prices_goerlil = source('prices', 'usd')
        , evms_info_goerlil = source('evms','info')
        , transfers_start_date = '2020-04-22'
        , blockchain = 'goerli'
    )
}}
