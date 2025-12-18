{{ config(
    materialized = 'incremental',
    schema = 'desyn',
    tags = ['desyn'],
    partition_by = ['day'],
    unique_key = ['day', 'blockchain', 'version'],
    incremental_strategy = 'merge',
    file_format = 'delta',
) }}

SELECT
    day,
    blockchain,
    version,
    SUM(protocol_liquidity_usd) AS protocol_liquidity_usd
FROM (
    SELECT * FROM {{ ref('desyn_arbitrum_liquidity_v1') }}
    UNION ALL
    SELECT * FROM {{ ref('desyn_bnb_liquidity_v1') }}
    UNION ALL
    SELECT * FROM {{ ref('desyn_ethereum_liquidity_v1') }}
    UNION ALL
    SELECT * FROM {{ ref('desyn_linea_liquidity_v1') }}
    UNION ALL
    SELECT * FROM {{ ref('desyn_hemi_liquidity_v1') }}
    UNION ALL
    SELECT * FROM {{ ref('desyn_plume_liquidity_v1') }}
    UNION ALL
    SELECT * FROM {{ ref('desyn_scroll_liquidity_v1') }}
    
) 
{% if is_incremental() %}
WHERE day >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY 1, 2, 3