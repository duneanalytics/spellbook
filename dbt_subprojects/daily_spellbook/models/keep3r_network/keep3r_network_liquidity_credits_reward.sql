{{ config(
    schema = 'keep3r_network'
    , alias = 'liquidity_credits_reward'
    , post_hook = '{{ expose_spells(\'["ethereum", "optimism", "polygon"]\',
                                "project", 
                                "keep3r",
                                 \'["0xr3x"]\') }}'
) }}


    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      _currentCredits,
      _job,
      _periodCredits,
      _rewardedAt,
      'ethereum' as blockchain, 
      0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44 AS token
    FROM
      {{ source(
        'keep3r_network_ethereum',
        'Keep3r_evt_LiquidityCreditsReward'
      ) }}
    UNION
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      _currentCredits,
      _job,
      _periodCredits,
      _rewardedAt,
      'ethereum' as blockchain,
      0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44 AS token
    FROM
      {{ source(
        'keep3r_network_ethereum',
        'Keep3r_v2_evt_LiquidityCreditsReward'
      ) }}
    UNION
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      _currentCredits,
      _job,
      _periodCredits,
      _rewardedAt,
        'optimism' as blockchain,
        0xca87472dbfb041c2e5a2672d319ea6184ad9755e as token
    FROM
      {{ source(
        'keep3r_network_optimism',
        'Keep3rSidechain_evt_LiquidityCreditsReward'
      ) }}
    UNION
    SELECT 
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      _currentCredits,
      _job,
      _periodCredits,
      _rewardedAt,
        'polygon' as blockchain,
        0x4a2be2075588bce6a7e072574698a7dbbac39b08 as token
    FROM
      {{ source(
        'keep3r_network_polygon',
        'Keep3rSidechain_evt_LiquidityCreditsReward'
      ) }}
