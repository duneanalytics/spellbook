{{
    config(
        
        alias = 'sandwich_attackers'
        , post_hook='{{ hide_spells() }}'
    )
}}

SELECT * FROM {{ ref('labels_sandwich_attackers_ethereum') }}