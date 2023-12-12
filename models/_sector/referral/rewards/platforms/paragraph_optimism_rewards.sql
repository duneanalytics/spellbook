{{ config(
    schema = 'paragraph_optimism',
    alias = 'referrer_mint_stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['mintReferrer', 'mint_date'],
    incremental_predicates = [incremental_predicate('call_block_time')],
    )
}}

WITH referrer_mint_stats AS (
    SELECT
        mintReferrer,
        DATE(call_block_time) AS mint_date,
        COUNT(*) AS total_mints,
        COUNT(CASE WHEN call_success THEN 1 ELSE NULL END) AS successful_mints,
        COUNT(CASE WHEN NOT call_success THEN 1 ELSE NULL END) AS failed_mints,
        SUM(CASE
                WHEN mintReferrer <> '0x0000000000000000000000000000000000000000' THEN 0.000111 -- Recompensa para creator referrer
                ELSE 0.000222 
            END) AS total_reward
    FROM {{ source('paragraph_optimism', 'ERC721_call_mintWithReferrer') }}
    GROUP BY mintReferrer, DATE(call_block_time)
)

SELECT
    mintReferrer,
    mint_date,
    total_mints,
    successful_mints,
    failed_mints,
    ROUND((successful_mints * 100.0) / NULLIF(total_mints, 0), 2) AS success_rate,
    total_reward
FROM referrer_mint_stats
WHERE call_block_time >= (SELECT MAX(call_block_time) FROM {{ this }}) 
ORDER BY mint_date DESC, success_rate DESC, total_mints DESC;

{% if is_incremental() %}
    and {{ incremental_predicate('call_block_time') }}
{% endif %}
