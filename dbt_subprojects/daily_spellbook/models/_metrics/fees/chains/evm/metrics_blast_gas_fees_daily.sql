{% set blockchain = 'blast' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        , tags=['static']
        , post_hook='{{ hide_spells() }}'
        )
}}


{{ metrics_fees_evm(blockchain) }}
