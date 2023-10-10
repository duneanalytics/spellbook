{{config(
    tags = ['dunesql', 'prod_exclude']
    , alias = alias('airdrop_2_receivers_optimism')
)}}

SELECT
    'optimism' as blockchain,
    address as address,
    '$OP Airdrop 2 Receiver' AS name,
    'airdrop' AS category,
    'hosuke' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-10-02' as created_at,
    now() as updated_at,
    'op_airdrop_2_receivers' AS model_name,
    'persona' as label_type
FROM {{ source('dune_upload', 'op_airdrop2_addresses_detailed_list') }}