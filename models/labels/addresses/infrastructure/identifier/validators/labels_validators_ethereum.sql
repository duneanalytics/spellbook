{{config(alias = alias('validators_ethereum'))}}

SELECT distinct
    'ethereum' as blockchain,
    from AS address,
    'Ethereum Validator' as name,
    'infrastructure' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-11') as created_at,
    now() as updated_at,
    'validators_ethereum' as model_name,
    'identifier' as label_type
FROM {{ source('ethereum','traces') }}
WHERE to = lower('0x00000000219ab540356cBB839Cbe05303d7705Fa')
AND success
AND value > 0