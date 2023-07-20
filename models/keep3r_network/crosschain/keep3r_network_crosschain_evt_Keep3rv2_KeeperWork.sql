{{ config(
    tags=['dunesql'],
    alias = alias('keep3r_network_Keep3r_v2_evt_KeeperWork'),
    unique_key = ['tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum", "optimism", "polygon"]\',
                                "sector",
                                "oracle",
                                \'["m_r_g_t"]\') }}'
    )
}}

SELECT
  'optimism' AS blockchain,
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  'KeeperWork' as event,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _payment AS payment,
  evt_tx_hash AS tx_hash
FROM
  source('keep3r_network_optimism','Keep3rSidechain_evt_KeeperWork')
UNION ALL
SELECT
  'polygon' AS blockchain,
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  'KeeperWork' as event,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _payment AS payment,
  evt_tx_hash AS tx_hash
FROM
  source('keep3r_network_polygon','Keep3rSidechain_evt_KeeperWork')
UNION ALL
SELECT
  'ethereum' AS blockchain,
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  'KeeperWork' as event,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _payment AS payment,
  evt_tx_hash AS tx_hash
FROM
  source('keep3r_network_ethereum','Keep3r_v2_evt_KeeperWork')
UNION ALL
SELECT
  'ethereum' AS blockchain,
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  'KeeperWork' as event,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _amount AS payment,
  evt_tx_hash AS tx_hash
FROM
  source('keep3r_network_ethereum','Keep3r_evt_KeeperWork')