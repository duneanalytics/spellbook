{{config(alias = alias('validators_solana'))}}

SELECT distinct
    'solana' as blockchain,
    recipient AS address,
    'Solana Validator' as name,
    'infrastructure' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-11') as created_at,
    now() as updated_at,
    'validators_solana' as model_name,
    'identifier' as label_type
FROM {{ source('solana','rewards') }}
where reward_type = "Voting"