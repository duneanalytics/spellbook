{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_darma_capital',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

SELECT txs.to AS depositor_address
, 'DARMA Capital' AS entity
, CONCAT('DARMA Capital ', CAST(ROW_NUMBER() OVER (ORDER BY MIN(txs.block_time)) AS VARCHAR)) AS entity_unique_name
, 'Staking Pool' AS category
FROM {{ source('ethereum', 'transactions') }} txs
INNER JOIN {{ source('ethereum', 'transactions') }} txs2 ON txs."from" = 0x7bf6583ec7f7b507e6d0d439901c4a0047936fd7
    AND txs2."from"=txs.to
    AND txs2.to = 0x00000000219ab540356cbb839cbe05303d7705fa
    AND txs2.block_number > txs.block_number
    {% if not is_incremental() %}
    AND txs.block_time >= date('2021-05-21')
    {% endif %}
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
GROUP BY 1