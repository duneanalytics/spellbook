{{ config(
    schema = 'social',
    alias = 'trades',
    partition_by = ['block_month', 'blockchain'],
    file_format = 'delta',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["base", "avalanche_c", "arbitrum", "bnb"]\',
                                "sector",
                                "social",
                                \'["hildobby"]\') }}'
    )
}}

{{
    enrich_social_trades
    (
        base_trades = ref('social_base_trades')
    )
}}