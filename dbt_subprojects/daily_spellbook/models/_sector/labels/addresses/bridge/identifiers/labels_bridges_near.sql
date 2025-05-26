{{config(
        materialized = 'view',
        alias = 'bridges_near',
        post_hook='{{ expose_spells(\'["near"]\',
                                    "sector",
                                    "labels",
                                    \'["Sector920"]\') }}')}}

SELECT 
    blockchain
    , to_utf8(address) as address
    , name
    , category
    , contributor
    , source, created_at
    , updated_at
    , model_name
    , label_type
FROM {{ ref('bridges_near_native')}}