{{ config(
    alias = alias('rewards_transfers'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "coinbase_wallet_quests",
                                \'["msilb7"]\') }}'
    )
}}
--quests started soft-launching in feb 2023, so set a buffer
{% set project_start_date = '2023-01-01' %}

WITH distributions AS (

SELECT qa.distributor_address
     , qa.quest_name
     , qa.rewards_token
     , r.to               AS quester_address
     , r.evt_tx_hash      AS tx_hash
     , r.evt_index
     , r.evt_block_time   AS block_time
     , r.evt_block_number AS block_number
     , r.value            AS rewards_token_value_raw
FROM {{source('erc20_optimism','evt_Transfer')}} r
INNER JOIN {{ref('coinbase_wallet_quests_optimism_distributor_addresses')}} qa
    ON r.`from` = distributor_address
    AND r.contract_address = rewards_token

WHERE evt_block_time >= cast('{{project_start_date}}' as timestamp) --arbitrary
{% if is_incremental() %}
-- for quest addresses we've seen before, pull incremental, else pull everything (controls for if we first see a distributor address later)
AND 1 = (
        CASE WHEN evt_block_time >= date_trunc('day', now() - interval '1 week') THEN 1
             WHEN distributor_address NOT IN (SELECT distributor_address FROM {{this}} GROUP BY 1) THEN 1--we don't have this loaded in yet.
             ELSE 0
        END
        )
{% endif %}

)

SELECT
DATE_TRUNC('day',block_time) AS block_date
, distributor_address, rewards_token, quest_name, quester_address
, tx_hash, evt_index, block_time, block_number
, rewards_token_value_raw

FROM distributions d

GROUP BY 1,2,3,4,5,6,7,8,9,10 --distinct