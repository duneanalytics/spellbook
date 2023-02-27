CREATE OR REPLACE VIEW pooltogether_v4_ethereum.prize_structure(

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
      pooltogether_v4_ethereum.PrizeTierHistoryV2_call_getPrizeTier
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
      pooltogether_v4_ethereum.PrizeDistributionBuffer_evt_PrizeDistributionSet
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
      split_part(tiers, ',', 1) AS tiers1,
      split_part(tiers, ',', 2) AS tiers2,
      split_part(tiers, ',', 3) AS tiers3,
      split_part(tiers, ',', 4) AS tiers4,
      split_part(tiers, ',', 5) AS tiers5,
      split_part(tiers, ',', 6) AS tiers6,
      split_part(tiers, ',', 7) AS tiers7,
      split_part(tiers, ',', 8) AS tiers8,
      split_part(tiers, ',', 9) AS tiers9,
      split_part(tiers, ',', 10) AS tiers10,
      split_part(tiers, ',', 11) AS tiers11,
      split_part(tiers, ',', 12) AS tiers12,
      split_part(tiers, ',', 13) AS tiers13,
      split_part(tiers, ',', 14) AS tiers14,
      split_part(tiers, ',', 15) AS tiers15,
      split_part(tiers, ',', 16) AS tiers16,
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
      priz
from
  detailedPrizeDistribution
)