{{config(

        alias = 'cex_optimism'
        , post_hook='{{ hide_spells() }}'
    )
}}

SELECT blockchain
, address
, distinct_name AS name
, 'institution' AS category
, added_by AS contributor
, 'static' AS source
, added_date AS created_at
, NOW() AS updated_at
, 'cex_' || blockchain AS model_name
, 'identifier' AS label_type
FROM {{ source('cex','addresses') }}
WHERE blockchain = 'optimism'
