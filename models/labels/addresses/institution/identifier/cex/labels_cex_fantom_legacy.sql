{{config(
        alias = alias('cex_fantom', legacy_model=True),
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "labels",
                                    \'["Henrystats"]\') }}')}}

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
FROM {{ ref('cex_fantom_addresses_legacy') }}