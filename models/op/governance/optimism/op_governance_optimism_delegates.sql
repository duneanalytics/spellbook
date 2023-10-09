{{ config(
        tags=['dunesql']
        , schema = 'op_governance_optimism'
        , alias = alias('delegates')
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['block_time', 'tx_hash', 'evt_index']
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_governance",
                                  \'["kaiblade"]\') }}'
  )
}}


WITH 
{% if is_incremental() %}
incremental_delegate_vote_data AS
(SELECT distinct(delegate)
FROM {{ source('op_optimism', 'GovernanceToken_evt_DelegateVotesChanged') }}
WHERE evt_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
),
{% endif %}

delegate_votes_data_raw AS
(SELECT evt_tx_hash AS tx_hash,
evt_block_time AS block_time,
evt_block_number AS block_number,
evt_index,
delegate,
CAST(newBalance AS DOUBLE)/1e18 AS newBalance, 
CAST(previousBalance AS DOUBLE)/1e18 AS previousBalance,
CAST(newBalance AS DOUBLE)/1e18 - CAST(previousBalance AS DOUBLE)/1e18 AS power_diff
FROM {{ source('op_optimism', 'GovernanceToken_evt_DelegateVotesChanged') }} AS hist
WHERE CAST(evt_block_time AS DATE) >= DATE'2022-05-26'
{% if is_incremental() %}
    AND EXISTS (
    SELECT increment.delegate
    FROM incremental_delegate_vote_data AS increment
    WHERE increment.delegate = hist.delegate
    )
{% endif %}
),

rolling_voting_power AS
(SELECT *,
SUM(power_diff) OVER (ORDER BY block_time) AS total_voting_power
FROM delegate_votes_data_raw
-- {% if is_incremental() %}
--     WHERE block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
-- {% endif %}
),

voting_power_share_data AS
(SELECT *, 
(CAST(newBalance AS DOUBLE)/CAST(total_voting_power AS DOUBLE)) * 100 AS voting_power_share
FROM rolling_voting_power
-- {% if is_incremental() %}
--     WHERE block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
-- {% endif %}
),

incremental_delegator_data AS
(SELECT *
FROM  {{ source('op_optimism', 'GovernanceToken_evt_DelegateChanged') }}
{% if is_incremental() %}
    WHERE evt_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
{% endif %}
), 

combined_delegator_count AS
(SELECT evt_tx_hash AS tx_hash, 
evt_block_time AS block_time, 
evt_block_number AS block_number,
evt_index,
fromDelegate AS delegate, 
-1 AS delegator_count
FROM incremental_delegator_data 
WHERE fromDelegate != 0x0000000000000000000000000000000000000000
AND CAST(evt_block_time AS DATE) >= DATE'2022-05-26'


UNION 

SELECT evt_tx_hash AS tx_hash, 
evt_block_time AS block_time, 
evt_block_number AS block_number,
evt_index, 
toDelegate AS delegate, 
1 AS delegator_count
FROM incremental_delegator_data
WHERE CAST(evt_block_time AS DATE) >= DATE'2022-05-26'

),

delegator_count_data AS
(SELECT tx_hash, block_time, block_number, evt_index, delegate, 
SUM(delegator_count) OVER (PARTITION BY delegate ORDER BY block_time) AS number_of_delegators,
SUM(delegator_count) OVER (ORDER BY block_time) AS total_delegators
FROM combined_delegator_count
-- {% if is_incremental() %}
--     WHERE block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
-- {% endif %}
),

votingPower_delegators_data AS
(SELECT power.*, del.number_of_delegators, 
del.total_delegators
FROM voting_power_share_data power
LEFT JOIN delegator_count_data del
ON power.delegate = del.delegate
AND power.tx_hash = del.tx_hash
AND power.block_number = del.block_number
-- {% if is_incremental() %}
--     AND power.block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
-- {% endif %}
),

votingPower_delegators_data_revised AS
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
FROM votingPower_delegators_data
-- {% if is_incremental() %}
--     WHERE block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
-- {% endif %}
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
FROM votingPower_delegators_data_revised
-- {% if is_incremental() %}
--     WHERE block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
-- {% endif %}
), 

op_delegates_table AS
(SELECT *, 
(CAST(number_of_delegators AS DOUBLE) / CAST(total_delegators AS DOUBLE))*100 AS total_delegators_share
FROM OP_delegates_table_raw
-- {% if is_incremental() %}
--     WHERE block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
-- {% endif %}
)


SELECT *
FROM op_delegates_table

-- FROM delegate_votes_data_raw 

