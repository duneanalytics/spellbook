{{ config(
    tags=['dunesql'],
    alias = alias('accounts'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "worldcoin",
                                    \'["msilb7"]\') }}')}}

-- Source of TX From & Logic 
-- https://cointelegraph.com/news/worldcoin-confirms-it-is-the-cause-of-mysterious-safe-deployments
WITH wld_deployers AS (
    SELECT
    to as worldcoin_deployer_address
    FROM optimism.transactions
    WHERE "from" = 0x86c5608362b3fbbeb721140472229392f754ef87
    AND value > cast(0 as uint256)
    AND block_number >= 105870092 --first transfer
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1
)

SELECT 
worldcoin_deployer_address
, ct.address AS account_address
, ct.block_time AS created_time
, ct.tx_hash AS creation_tx_hash
, ct.block_number AS created_block_number

FROM wld_deployers w
INNER JOIN optimism.transactions t
    ON t."from" = worldcoin_deployer_address
    AND t.block_number >= 105870092 --first transfer
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN optimism.creation_traces ct
    ON ct.tx_hash = t.hash
    AND ct.block_number = t.block_number
    AND ct.block_number >= 105870092 --first transfer
    AND ct."from" IN (  --safe factories
                        0xa6b71e26c5e0845f74c812102ca7114b6a896ab2
                        ,0xc22834581ebc8527d974f8a1c97e1bea4ef910bc
                        )
    {% if is_incremental() %}
    AND ct.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

WHERE 1=1
{% if is_incremental() %}
AND t.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}