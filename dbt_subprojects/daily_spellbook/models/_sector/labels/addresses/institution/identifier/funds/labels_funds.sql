{{config(
        
        alias = 'funds'
        , post_hook='{{ hide_spells() }}'
)}}

SELECT * FROM {{ ref('labels_funds_ethereum') }}
