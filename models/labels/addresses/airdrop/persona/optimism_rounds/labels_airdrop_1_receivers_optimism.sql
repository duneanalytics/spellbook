{{config(tags=['dunesql'],alias = alias('airdrop_1_receivers_optimism'))}}

SELECT
    'optimism' as blockchain,
    from_hex(address) as address,
    '$OP Airdrop 1 Receiver' AS name,
    'airdrop' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    DATE '2022-09-29' as created_at,
    now() as updated_at,
    'op_airdrop_1_receivers' AS model_name,
    'persona' as label_type
FROM { ref('airdrop_optimism_addresses_1_legacy') } -- {{ ref('airdrop_optimism_addresses_1') }}