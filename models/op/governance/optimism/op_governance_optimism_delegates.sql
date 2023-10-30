{{ config(
     schema = 'op_governance_optimism'
        , alias = 'delegates'
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_governance",
                                  \'["kaiblade"]\') }}'
  )
}}


WITH rolling_voting_power AS
(SELECT *,
SUM(power_diff) OVER (ORDER BY block_time) AS total_voting_power
FROM {{ ref('op_governance_optimism_voting_power') }}
),

voting_power_share_data AS
(SELECT *, 
(CAST(newBalance AS DOUBLE)/CAST(total_voting_power AS DOUBLE)) * 100 AS voting_power_share
FROM rolling_voting_power
),

combined_delegator_count AS
(SELECT tx_hash, 
block_time, 
block_number,
evt_index,
fromDelegate AS delegate, 
-1 AS delegator_count
FROM {{ ref('op_governance_optimism_delegators') }}
WHERE fromDelegate != 0x0000000000000000000000000000000000000000
AND CAST(block_time AS DATE) >= DATE'2022-05-26'

UNION 

SELECT tx_hash, 
block_time, 
block_number,
evt_index, 
toDelegate AS delegate, 
1 AS delegator_count
FROM {{ ref('op_governance_optimism_delegators') }}
WHERE CAST(block_time AS DATE) >= DATE'2022-05-26'
),

delegator_count_data AS
(SELECT tx_hash, block_time, block_number, evt_index, delegate, 
SUM(delegator_count) OVER (PARTITION BY delegate ORDER BY block_time) AS number_of_delegators,
SUM(delegator_count) OVER (ORDER BY block_time) AS total_delegators
FROM combined_delegator_count
),

voting_power_delegators_data AS
(SELECT power.*, del.number_of_delegators, 
del.total_delegators
FROM voting_power_share_data power
LEFT JOIN delegator_count_data del
ON power.delegate = del.delegate
AND power.tx_hash = del.tx_hash
AND power.block_number = del.block_number
),

voting_power_delegators_data_revised AS
(SELECT 
tx_hash,
block_time, 
block_number,
evt_index,
delegate,
newBalance, 
previousBalance,
power_diff,
(CASE
WHEN previousBalance = 0 THEN NULL
ELSE (power_diff/previousBalance)*100
END) AS voting_power_change,
total_voting_power,
voting_power_share,
LAST_VALUE(number_of_delegators) IGNORE NULLS OVER (PARTITION BY delegate ORDER BY block_time) AS number_of_delegators,
LAST_VALUE(total_delegators) IGNORE NULLS OVER (ORDER BY block_time) AS total_delegators
FROM voting_power_delegators_data
),

OP_delegates_table_raw AS
(SELECT block_time, 
tx_hash,
evt_index,
delegate,
newBalance AS current_voting_power,
previousBalance AS previous_voting_power,
power_diff,
voting_power_change,
total_voting_power,
voting_power_share,
COALESCE(number_of_delegators,1) AS number_of_delegators,
total_delegators
FROM voting_power_delegators_data_revised
), 

OP_delegates_table AS
(SELECT *, 
(CAST(number_of_delegators AS DOUBLE) / CAST(total_delegators AS DOUBLE))*100 AS total_delegators_share
FROM OP_delegates_table_raw
)

SELECT *
FROM OP_delegates_table
