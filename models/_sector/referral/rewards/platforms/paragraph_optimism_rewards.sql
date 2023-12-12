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
        DATE(paragraph_optimism.ERC721_call_mintWithReferrer.call_block_time) AS mint_date,
        COUNT(*) AS total_mints,
        COUNT(CASE WHEN paragraph_optimism.ERC721_call_mintWithReferrer.call_success THEN 1 ELSE NULL END) AS successful_mints,
        COUNT(CASE WHEN NOT paragraph_optimism.ERC721_call_mintWithReferrer.call_success THEN 1 ELSE NULL END) AS failed_mints,
        SUM(
            CASE
                WHEN paragraph_optimism.ERC721_call_getMintFee.contract_address = paragraph_optimism.ERC721_call_mintWithReferrer.contract_address 
                     AND paragraph_optimism.ERC721_call_getMintFee.output_0 > 0 THEN 
                    CASE
                        WHEN mintReferrer = CAST('0x0000000000000000000000000000000000000000' AS varbinary) THEN 0.000444
                        ELSE 0.000222
                    END
                ELSE -- Grátis
                    CASE
                        WHEN mintReferrer = CAST('0x0000000000000000000000000000000000000000' AS varbinary) THEN 0.000333
                        ELSE 0.000111
                    END
            END
        ) AS total_reward
    FROM paragraph_optimism.ERC721_call_mintWithReferrer
    LEFT JOIN paragraph_optimism.ERC721_call_getMintFee ON paragraph_optimism.ERC721_call_mintWithReferrer.contract_address = paragraph_optimism.ERC721_call_getMintFee.contract_address
    GROUP BY mintReferrer, DATE(paragraph_optimism.ERC721_call_mintWithReferrer.call_block_time)
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
