{{ config(
    alias = alias('erc721_agg_day'),
    materialized ='incremental',
    file_format ='delta',
    incremental_strategy='merge',
    unique_key='unique_transfer_id',
    tags=['dunesql'])
}}

SELECT
    'ethereum' AS blockchain,
    date_trunc('day', evt_block_time) AS day,
    wallet_address,
    token_address,
    tokenId,
    concat(cast(wallet_address AS varchar), '-', to_iso8601(date_trunc('day', evt_block_time)), '-', CAST(token_address AS varchar), '-', cast(tokenId AS varchar)) AS unique_transfer_id,
    sum(amount) AS amount
FROM {{ ref('transfers_ethereum_erc721') }}
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE evt_block_time >= date_trunc('day', cast(now() AS timestamp) - interval '7' day)
{% endif %}
GROUP BY 1,2,3,4,5,6
-- having sum(amount) = 1 commenting this out as it seems to affect the rolling models
