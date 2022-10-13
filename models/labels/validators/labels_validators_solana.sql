{{config(alias='validators_solana')}}

SELECT distinct
    array('solana') as blockchain,
    recipient AS address,
    'Solana Validator' as name,
    'validators' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-11') as created_at,
    now() as updated_at
FROM {{ source('solana','rewards') }}
where reward_type = "Voting"