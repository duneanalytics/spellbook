{% macro dex_crossdomain_mev(blockchain, blocks, traces, transactions) %}

WITH received_by_builder AS (
    SELECT b.number AS block_number
    , b.time AS block_time
    , array_distinct(COALESCE(array_agg(traces."from"), NULL) || COALESCE(array_agg(erc."from"), NULL)) AS senders
    FROM {{blocks}} b
    LEFT JOIN {{traces}} traces ON traces.block_number=b.number
        AND b.miner=traces.to
        AND traces.success
        AND traces.value > UINT256 '0'
        {% if is_incremental() %}
        AND traces.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    LEFT JOIN {{source('erc20_' + blockchain, 'evt_transfer')}}  erc ON erc.evt_block_number=b.number
        AND b.miner=erc.to
        AND erc.value > UINT256 '0'
        {% if is_incremental() %}
        AND erc.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    {% if is_incremental() %}
    WHERE b.time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2
    )


SELECT distinct dt.blockchain
, dt.project
, dt.version
, dt.block_time
, CAST(date_trunc('month', txs.block_time) AS date) AS block_month
, txs.block_number
, dt.token_sold_address
, dt.token_bought_address
, dt.token_sold_symbol
, dt.token_bought_symbol
, dt.maker
, dt.taker
, dt.tx_hash
, dt.tx_from
, dt.tx_to
, dt.project_contract_address
, dt.token_pair
, txs.index
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, dt.evt_index
FROM {{ ref('dex_trades') }} dt
INNER JOIN received_by_builder rbb ON rbb.block_time=dt.block_time
    AND (contains(rbb.senders, dt.tx_from) OR contains(rbb.senders, dt.taker))
INNER JOIN {{transactions}} txs ON txs.block_time=dt.block_time
    AND txs.hash=dt.tx_hash
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE dt.blockchain='{{blockchain}}'
{% if is_incremental() %}
AND dt.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

{% endmacro %}