{{config(
	tags=['legacy'],
	alias = alias('airdrop_1_receivers_optimism', legacy_model=True))}}

SELECT
    'optimism' as blockchain,
    address,
    '$OP Airdrop 1 Receiver' AS name,
    'airdrop' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-09-29') as created_at,
    now() as updated_at,
    'op_airdrop_1_receivers' AS model_name,
    'persona' as label_type
FROM {{ ref('airdrop_optimism_addresses_1_legacy') }}