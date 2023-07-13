{{config(
        alias = alias('cex_avalanche_c', legacy_model=True),
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

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
FROM {{ ref('cex_avalanche_c_addresses_legacy') }}