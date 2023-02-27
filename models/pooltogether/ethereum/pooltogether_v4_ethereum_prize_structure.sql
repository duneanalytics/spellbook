{{ config(
  alias ='prize_structure'
  post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "pooltogether_v4",
                                \'["bronder"]\') }}'
)}}

{% set uniswap_models = [
'pooltogether_v4_ethereum_prize_structure'
] %}

   WITH
  --Calculate proze structure for Ethereum network per drawID
  prizeDistribution AS (
    --ETHEREUM POST DPR
    SELECT
      call_tx_hash AS tx_hash,
      call_block_time AS block_time,
      'Ethereum' AS network,
      drawId,
      json_query(output_0, 'lax $.bitRangeSize') AS bitRange,
      substr(
        json_query(output_0, 'lax $.tiers'),
        2,
        length(json_query(output_0, 'lax $.tiers')) -2
      ) AS tiers,
      CAST(json_query(output_0, 'lax $.dpr') AS int) / power(10, 6) AS dpr,
      CAST(json_query(output_0, 'lax $.prize') AS double) / power(10, 6) AS prize
    FROM
      {{ source('pooltogether_v4_ethereum', 'PrizeTierHistoryV2_call_getPrizeTier')}}
    WHERE
      call_success = true
      --ETHEREUM PRE DPR
    UNION ALL
    SELECT
      evt_tx_hash AS tx_hash,
      evt_block_time,
      'Ethereum' AS network,
      drawId,
      json_query(prizeDistribution, 'lax $.bitRangeSize') AS bitRange,
      substr(
        json_query(prizeDistribution, 'lax $.tiers'),
        2,
        length(json_query(prizeDistribution, 'lax $.tiers')) -2
      ) AS tiers,
      0 AS dpr,
      CAST(
        json_query(prizeDistribution, 'lax $.prize') AS double
      ) / power(10, 6) AS prize
    FROM
      {{ source('pooltogether_v4_ethereum', 'PrizeDistributionBuffer_evt_PrizeDistributionSet')}}
    WHERE
      drawID < 447 --DPR On Ethereum started on draw 447
  ),
  detailedPrizeDistribution AS (
    SELECT
      tx_hash,
      block_time,
      network,
      drawId,
      CAST(bitRange AS int) AS bitRange,
      CAST(split_part(tiers, ',', 1) AS int) AS tiers1,
      CAST(split_part(tiers, ',', 2) AS int) AS tiers2,
      CAST(split_part(tiers, ',', 3) AS int) AS tiers3,
      CAST(split_part(tiers, ',', 4) AS int) AS tiers4,
      CAST(split_part(tiers, ',', 5) AS int) AS tiers5,
      CAST(split_part(tiers, ',', 6) AS int) AS tiers6,
      CAST(split_part(tiers, ',', 7) AS int) AS tiers7,
      CAST(split_part(tiers, ',', 8) AS int) AS tiers8,
      CAST(split_part(tiers, ',', 9) AS int) AS tiers9,
      CAST(split_part(tiers, ',', 10) AS int) AS tiers10,
      CAST(split_part(tiers, ',', 11) AS int) AS tiers11,
      CAST(split_part(tiers, ',', 12) AS int) AS tiers12,
      CAST(split_part(tiers, ',', 13) AS int) AS tiers13,
      CAST(split_part(tiers, ',', 14) AS int) AS tiers14,
      CAST(split_part(tiers, ',', 15) AS int) AS tiers15,
      CAST(split_part(tiers, ',', 16) AS int) AS tiers16,
      dpr,
      prize
    FROM
      prizeDistribution
  )
select
  tx_hash,
      block_time,
      network,
      drawId,
      bitRange,
      tiers1,
      tiers2,
      tiers3,
      tiers4,
      tiers5,
      tiers6,
      tiers7,
      tiers8,
      tiers9,
      tiers10,
      tiers11,
      tiers12,
      tiers13,
      tiers14,
      tiers15,
      tiers16,
      dpr,
      prize
from
  detailedPrizeDistribution