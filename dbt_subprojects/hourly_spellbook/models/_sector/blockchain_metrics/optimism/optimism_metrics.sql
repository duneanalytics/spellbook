{{ config(
        schema='optimism',
        alias = 'metrics',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','block_hour'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')],
        post_hook='{{ expose_spells(\'["optimism"]\',
                "sector",
                "metrics",
                \'["0xRob"]\') }}')
}}

{{blockchain_metrics('optimism')}}
