{{ config(
        schema='celo',
        alias = 'token_transfer_metrics',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','block_hour', 'token_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_hour')],
        post_hook='{{ expose_spells(\'["celo"]\',
                "sector",
                "metrics",
                \'["0xRob"]\') }}')
}}

{{blockchain_token_transfer_metrics('celo')}}
