{% set blockchain = 'blast' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily'
        , materialized = 'view'
        , tags=['static']
        , post_hook='{{ hide_spells() }}'
        )
}}

SELECT *
FROM {{ source('tokens_blast', 'net_transfers_daily') }}
