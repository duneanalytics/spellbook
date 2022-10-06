{{config(alias='ens',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["0xRob"]\') }}')
}}



SELECT array('ethereum') as blockchain,
       address,
       name,
       'ENS resolver' as category,
       '0xRob' as contributor,
       'query' AS source,
       date('2022-10-06') as created_at,
       now() as modified_at
FROM {{ ref('ens_resolver_latest') }}
UNION
SELECT array('ethereum') as blockchain,
       address,
       name,
       'ENS reverse' as category,
       '0xRob' as contributor,
       'query' AS source,
       date('2022-10-06') as created_at,
       now() as modified_at
FROM {{ ref('ens_resolver_latest') }}
