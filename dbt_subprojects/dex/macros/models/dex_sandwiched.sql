{% macro dex_sandwiched(blockchain, transactions, sandwiches) %}

WITH sandwich_bounds AS (
    SELECT front.block_time
    , front.evt_index AS min_evt_index
    , back.evt_index AS max_evt_index
    , front.project_contract_address    
    , front.token_bought_address
    , front.token_sold_address
    FROM {{sandwiches}} front
    INNER JOIN {{sandwiches}} back ON front.block_time=back.block_time
        AND front.tx_from=back.tx_from
        AND front.tx_hash!=back.tx_hash
        AND front.project_contract_address=back.project_contract_address
        AND front.token_sold_address=back.token_bought_address
        AND front.token_bought_address=back.token_sold_address
        AND front.evt_index+1 < back.evt_index
        {% if is_incremental() %}
        AND {{ incremental_predicate('back.block_time') }}
        {% endif %}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('front.block_time') }}
    {% endif %}
    )

SELECT DISTINCT dt.blockchain
, dt.project
, dt.version
, dt.block_time
, txs.block_number
, dt.block_month
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
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, dt.evt_index
, txs.index AS tx_index
FROM {{ ref('dex_trades') }} dt
INNER JOIN sandwich_bounds sb ON sb.block_time=dt.block_time
    AND sb.project_contract_address=dt.project_contract_address
    AND sb.token_bought_address=dt.token_bought_address
    AND sb.token_sold_address=dt.token_sold_address
    AND dt.evt_index BETWEEN sb.min_evt_index AND sb.max_evt_index
INNER JOIN {{transactions}} txs ON txs.block_time=dt.block_time
    AND txs.hash=dt.tx_hash
    {% if is_incremental() %}
    AND {{ incremental_predicate('txs.block_time') }}
    {% endif %}
WHERE dt.blockchain='{{blockchain}}'
{% if is_incremental() %}
AND {{ incremental_predicate('dt.block_time') }}
{% endif %}

{% endmacro %}