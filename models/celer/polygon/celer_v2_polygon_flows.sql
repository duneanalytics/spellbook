{{ config(
    alias = 'flows',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'transfer_id'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "celer",
                                \'["soispoke"]\') }}'
    )
}}

SELECT DISTINCT 'polygon' as blockchain,
       'celer' as project,
       'v2' as version,
       lower(cb.contract_address) as contract_address,
       evt_block_time as block_time,
       date_trunc('day', evt_block_time) AS block_date,
       evt_block_number as block_number,
       evt_tx_hash as tx_hash,
       evt_index,
       'deposit' as tx_type,
       sender,
       '' as receiver,
       lower(token) as token_address,
       p.symbol as token_symbol,
       CAST(amount AS DOUBLE) as token_amount_raw,
       CAST(amount / power(10,p.decimals) AS DOUBLE) as token_amount,
       CAST(amount /power(10,p.decimals) * p.price AS DOUBLE) as token_amount_usd,
       CAST(amount /power(10,p.decimals) * p.price / peth.price AS DOUBLE) as token_amount_native,
       transferId as transfer_id
FROM {{ source('celer_polygon', 'Bridge_evt_Send') }} cb
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', cb.evt_block_time)
                       AND lower(p.contract_address) = lower(cb.token)
                       {% if is_incremental() %}
                       AND p.minute >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} peth ON peth.minute = date_trunc('minute', cb.evt_block_time)
                       AND peth.symbol = 'WETH'
                       {% if is_incremental() %}
                       AND peth.minute >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
WHERE p.blockchain ='polygon' and peth.blockchain = 'polygon'
{% if is_incremental() %}
AND cb.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
                    UNION ALL
SELECT DISTINCT 'polygon' as blockchain,
       'celer' as project,
       'v2' as version,
       lower(cb.contract_address) as contract_address,
       evt_block_time as block_time,
       date_trunc('day', evt_block_time) AS block_date,
       evt_block_number as block_number,
       evt_tx_hash as tx_hash,
       evt_index,
       'withdrawal' as tx_type,
       '' as sender,
       receiver,
       lower(token) as token_address,
       p.symbol as token_symbol,
       amount as token_amount_raw,
       amount / power(10,p.decimals) as token_amount,
       amount /power(10,p.decimals) * p.price as token_amount_usd,
       amount /power(10,p.decimals) * p.price / peth.price as token_amount_eth,
       withdrawId as transfer_id
FROM {{ source('celer_polygon', 'Bridge_evt_WithdrawDone') }} cb
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', cb.evt_block_time)
                       AND lower(p.contract_address) = lower(cb.token)
                       {% if is_incremental() %}
                       AND p.minute >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} peth ON peth.minute = date_trunc('minute', cb.evt_block_time)
                       AND peth.symbol = 'WETH'
                       {% if is_incremental() %}
                       AND peth.minute >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
WHERE p.blockchain ='polygon' AND peth.blockchain = 'polygon'
{% if is_incremental() %}
AND cb.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}