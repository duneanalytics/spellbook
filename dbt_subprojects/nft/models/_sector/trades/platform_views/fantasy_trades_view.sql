
{{ config(
        schema = 'fantasy',
        alias = 'trades',

        materialized = 'view',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
        )
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'fantasy'
