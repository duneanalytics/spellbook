{{ config(
    tags=['dunesql'],
    alias = alias('accounts'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['account_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "worldcoin",
                                    \'["msilb7"]\') }}')}}

-- Source of TX From & Logic 
-- https://cointelegraph.com/news/worldcoin-confirms-it-is-the-cause-of-mysterious-safe-deployments
WITH wld_deployers AS (
    SELECT
    to as worldcoin_deployer_address
    FROM {{ source('optimism','transactions') }}
    WHERE "from" IN (
                 0x86c5608362b3fbbeb721140472229392f754ef87
                ,0x80dc00811e7c4a03c1f1599d3dc8febaad87bf87
                )
    AND value > cast(0 as uint256)
    AND block_number >= 105870092 --first transfer
    -- don't do incremental here, we want to always build this subaccount list (maybe an eventual spell)
    GROUP BY 1
)

SELECT 
worldcoin_deployer_address
, ct.address AS account_address
, ct.block_time AS created_time
, ct.tx_hash AS creation_tx_hash
, ct.block_number AS created_block_number

FROM wld_deployers w
INNER JOIN {{ source('optimism','transactions') }} t
    ON t."from" = worldcoin_deployer_address
    AND t.block_number >= 105870092 --first transfer
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{ source('optimism','creation_traces') }}  ct
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