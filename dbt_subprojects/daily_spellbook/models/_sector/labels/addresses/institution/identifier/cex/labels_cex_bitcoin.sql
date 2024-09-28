{{config(

        alias = 'cex_bitcoin',
        post_hook='{{ expose_spells(\'["bitcoin"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain
, from_base58(cast(address as varchar)) as address
, distinct_name AS name
, 'institution' AS category
, added_by AS contributor
, 'static' AS source
, added_date AS created_at
, NOW() AS updated_at
, 'cex_' || blockchain AS model_name
, 'identifier' AS label_type
FROM {{ source('cex','addresses') }}
WHERE blockchain = 'bitcoin'
AND regexp_like(cast(address as varchar), '^[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]+$')
