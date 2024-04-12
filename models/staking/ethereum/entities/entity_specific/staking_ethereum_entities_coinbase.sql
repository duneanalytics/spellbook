{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_coinbase',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

SELECT coinbase.address AS depositor_address
, 'Coinbase' AS entity
, CONCAT('Coinbase ', CAST(ROW_NUMBER() OVER (ORDER BY MIN(coinbase.block_time)) AS VARCHAR)) AS entity_unique_name
, 'CEX' AS category
, 'deposit_address' AS tagging_method
FROM (
        SELECT
            et."from" AS address
            , et.block_time
        FROM {{ source('ethereum', 'traces') }} et
        INNER JOIN {{ source('ethereum', 'traces') }} et2 ON et2."from"=et."from"
            AND et2.to IN (SELECT address FROM {{ ref('cex_ethereum_addresses') }} WHERE cex_name = 'Coinbase')
            {% if not is_incremental() %}
            AND et2.block_time >= DATE'2020-10-14'
            {% endif %}
            {% if is_incremental() %}
            AND et2.block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        WHERE et.to = 0x00000000219ab540356cbb839cbe05303d7705fa
            AND et.success
            {% if not is_incremental() %}
            AND et.block_time >= DATE '2020-10-14'
            {% endif %}
            {% if is_incremental() %}
            AND et.block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        AND et."from" != 0x1ef753934c40a72a60eab12a68b6f8854439aa78
        GROUP BY et."from", et.block_time
    ) coinbase
GROUP BY coinbase.address