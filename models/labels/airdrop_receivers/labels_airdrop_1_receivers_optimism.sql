{{config(alias='airdrop_1_receivers_optimism')}}

SELECT
    array('optimism') as blockchain,
    address,
    '$OP Airdrop 1 Receiver' AS name,
    'airdrop' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-09-29') as created_at,
    now() as updated_at
FROM {{ ref('airdrop_optimism_addresses_1') }}