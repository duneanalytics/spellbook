{{config(
        materialized = 'view',
        schema = 'labels',
        alias = 'bridges_near',
)}}

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
FROM {{ ref('labels_bridges_near_native')}}