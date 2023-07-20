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
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _payment AS payment,
  evt_tx_hash AS tx_hash
FROM
  keep3r_network_optimism.Keep3rSidechain_evt_KeeperWork
UNION ALL
SELECT
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _payment AS payment,
  evt_tx_hash AS tx_hash
FROM
  keep3r_network_polygon.Keep3rSidechain_evt_KeeperWork
UNION ALL
SELECT
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _payment AS payment,
  evt_tx_hash AS tx_hash
FROM
  keep3r_network_ethereum.Keep3r_v2_evt_KeeperWork
UNION ALL
SELECT
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  _credit AS credit,
  contract_address,
  evt_index,
  _gasLeft AS gasLeft,
  _job AS job,
  _keeper AS keeper,
  _amount AS payment,
  evt_tx_hash AS tx_hash
FROM
  keep3r_network_ethereum.Keep3r_evt_KeeperWork