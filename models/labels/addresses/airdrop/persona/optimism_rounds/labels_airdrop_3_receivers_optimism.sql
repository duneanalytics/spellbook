{{config(
    tags = ['dunesql']
    , alias = alias('airdrop_3_receivers_optimism')
)}}

SELECT
    'optimism' as blockchain,
    address as address,
    '$OP Airdrop 3 Receiver' AS name,
    'airdrop' AS category,
    'hosuke' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-10-02' as created_at,
    now() as updated_at,
    'op_airdrop_3_receivers' AS model_name,
    'persona' as label_type
FROM {{ source('dune_upload', 'op_airdrop_3_addresses_detailed_list') }}