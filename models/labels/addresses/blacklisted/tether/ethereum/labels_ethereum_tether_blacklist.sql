{{config(
    alias='tether_blacklist',
    tags=['static'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["hildobby"]\') }}'
)}}

SELECT 'ethereum' AS blockchain
, a._user AS address
, 'Tether Blacklist' AS name
, 'blacklisted' AS category
, 'hildobby' AS contributor
, 'query' AS source
, a.evt_block_time AS created_at
, NOW() AS updated_at
, 'blacklisted' AS model_name
, 'identifier' AS label_type
FROM tether_ethereum.Tether_USD_evt_AddedBlackList a
LEFT JOIN tether_ethereum.Tether_USD_evt_RemovedBlackList r ON a._user=r._user
    AND a.evt_block_number < r.evt_block_number
WHERE r.evt_index IS NULL