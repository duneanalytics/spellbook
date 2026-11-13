{{ config(
        schema='blast',
        alias = 'transaction_metrics',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}')
}}

{{blockchain_transaction_metrics('blast')}}
