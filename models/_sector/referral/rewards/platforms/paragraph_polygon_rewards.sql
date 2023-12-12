{{ config(
    schema = 'paragraph_polygon',
    alias = 'referrer_mint_stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['mintReferrer', 'mint_date'],
    incremental_predicates = [incremental_predicate('call_block_time')],
    )
}}

WITH mint_fee_info AS (
    SELECT 
        contract_address,
        output_0 
    FROM paragraph_polygon.ERC721_call_getMintFee
),
referrer_mint_stats AS (
    SELECT
        mwr.mintReferrer,
        DATE(mwr.call_block_time) AS mint_date,
        COUNT(*) AS total_mints,
        COUNT(CASE WHEN mwr.call_success THEN 1 ELSE NULL END) AS successful_mints,
        COUNT(CASE WHEN NOT mwr.call_success THEN 1 ELSE NULL END) AS failed_mints,
        SUM(
            CASE
                WHEN mfi.contract_address = mwr.contract_address AND mfi.output_0 > 0 THEN 
                    CASE
                        WHEN mwr.mintReferrer = CAST('0x0000000000000000000000000000000000000000' AS varbinary) THEN 0.000444
                        ELSE 0.000222
                    END
                ELSE
                    CASE
                        WHEN mwr.mintReferrer = CAST('0x0000000000000000000000000000000000000000' AS varbinary) THEN 0.000333
                        ELSE 0.000111
                    END
            END
        ) AS total_reward
    FROM paragraph_polygon.ERC721_call_mintWithReferrer mwr
    LEFT JOIN mint_fee_info mfi ON mwr.contract_address = mfi.contract_address
    GROUP BY mwr.mintReferrer, DATE(mwr.call_block_time)
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
WHERE referrer_mint_stats.call_block_time >= (SELECT MAX(call_block_time) FROM {{ this }}) 
ORDER BY mint_date DESC, success_rate DESC, total_mints DESC;

{% if is_incremental() %}
    {{ incremental_predicate('call_block_time') }}
{% endif %}
