{{ config(
	  tags=['legacy'],
    schema ='pooltogether_v4_ethereum',
    alias = alias('prize_structure', legacy_model=True),
    materialized = 'view'
)}}

WITH
  --Calculate prize structure for Ethereum network per drawID
prize_distribution AS (
    --ETHEREUM POST DPR
    SELECT call_tx_hash                                                        AS tx_hash,
           call_block_time                                                     AS block_time,
           'Ethereum'                                                          AS network,
           drawId,
           get_json_object(output_0, '$.bitRangeSize')                         AS bitRange,
           substr(
                   get_json_object(output_0, '$.tiers'),
                   2,
                   length(get_json_object(output_0, '$.tiers')) - 2
               )                                                               AS tiers,
           CAST(get_json_object(output_0, '$.dpr') AS int) / power(10, 6)      AS dpr,
           CAST(get_json_object(output_0, '$.prize') AS double) / power(10, 6) AS prize
    FROM
      {{ source('pooltogether_v4_ethereum', 'PrizeTierHistoryV2_call_getPrizeTier')}}
    WHERE call_success = true

    UNION ALL

    --ETHEREUM PRE DPR
    SELECT evt_tx_hash                                                                  AS tx_hash,
           evt_block_time,
           'Ethereum'                                                                   AS network,
           drawId,
           get_json_object(prizeDistribution, '$.bitRangeSize')                         AS bitRange,
           substr(
                   get_json_object(prizeDistribution, '$.tiers'),
                   2,
                   length(get_json_object(prizeDistribution, '$.tiers')) - 2
               )                                                                        AS tiers,
           0                                                                            AS dpr,
           CAST(get_json_object(prizeDistribution, '$.prize') AS double) / power(10, 6) AS prize
    FROM
      {{ source('pooltogether_v4_ethereum', 'PrizeDistributionBuffer_evt_PrizeDistributionSet')}}
    WHERE drawID < 447 --DPR On Ethereum started on draw 447
),

detailed_prize_distribution AS (
    SELECT tx_hash,
           block_time,
           network,
           drawId                                  AS draw_id,
           CAST(bitRange AS int)                   AS bit_range,
           CAST(split_part(tiers, ',', 1) AS int)  AS tiers1,
           CAST(split_part(tiers, ',', 2) AS int)  AS tiers2,
           CAST(split_part(tiers, ',', 3) AS int)  AS tiers3,
           CAST(split_part(tiers, ',', 4) AS int)  AS tiers4,
           CAST(split_part(tiers, ',', 5) AS int)  AS tiers5,
           CAST(split_part(tiers, ',', 6) AS int)  AS tiers6,
           CAST(split_part(tiers, ',', 7) AS int)  AS tiers7,
           CAST(split_part(tiers, ',', 8) AS int)  AS tiers8,
           CAST(split_part(tiers, ',', 9) AS int)  AS tiers9,
           CAST(split_part(tiers, ',', 10) AS int) AS tiers10,
           CAST(split_part(tiers, ',', 11) AS int) AS tiers11,
           CAST(split_part(tiers, ',', 12) AS int) AS tiers12,
           CAST(split_part(tiers, ',', 13) AS int) AS tiers13,
           CAST(split_part(tiers, ',', 14) AS int) AS tiers14,
           CAST(split_part(tiers, ',', 15) AS int) AS tiers15,
           CAST(split_part(tiers, ',', 16) AS int) AS tiers16,
           dpr,
           prize
    FROM prize_distribution
)

SELECT tx_hash,
       block_time,
       network,
       draw_id,
       bit_range,
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
FROM detailed_prize_distribution