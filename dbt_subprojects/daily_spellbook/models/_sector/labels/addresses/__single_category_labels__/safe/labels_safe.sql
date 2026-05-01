{{config(
    alias = 'safe'
    , post_hook='{{ hide_spells() }}'
)}}

SELECT * FROM {{ ref('labels_safe_ethereum') }}