{% macro dex_sandwiches(blockchain, transactions) %}

-- Checking that each frontrun trade has a matching backrun and at least one victim in between
SELECT DISTINCT dt.blockchain
, dt.project
, dt.version
, dt.block_time
, dt.block_month
, tx.block_number
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
, tx.index AS tx_index
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, dt.evt_index
FROM {{ ref('dex_trades') }} front
INNER JOIN {{ ref('dex_trades') }} back ON back.blockchain='{{blockchain}}'
    AND front.block_time=back.block_time
    AND front.project_contract_address=back.project_contract_address
    AND front.tx_from=back.tx_from
    AND front.tx_hash!=back.tx_hash
    AND front.token_sold_address=back.token_bought_address
    AND front.token_bought_address=back.token_sold_address
    AND front.evt_index + 1 < back.evt_index
    {% if is_incremental() %}
    AND back.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{ ref('dex_trades') }} victim ON victim.blockchain='{{blockchain}}'
    AND front.block_time=victim.block_time
    AND front.project_contract_address=victim.project_contract_address
    AND front.tx_from!=victim.tx_from
    AND front.token_bought_address=victim.token_bought_address
    AND front.token_sold_address=victim.token_sold_address
    AND victim.evt_index BETWEEN front.evt_index AND back.evt_index
    {% if is_incremental() %}
    AND victim.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{ ref('dex_trades') }} dt ON dt.blockchain='{{blockchain}}'
    AND dt.block_time=front.block_time
    AND dt.tx_hash IN (front.tx_hash, back.tx_hash)
    AND dt.project_contract_address=front.project_contract_address
    AND dt.evt_index IN (front.evt_index, back.evt_index)
    AND ((dt.token_sold_address=front.token_sold_address AND dt.token_bought_address=front.token_bought_address)
        OR (dt.token_sold_address=front.token_bought_address AND dt.token_bought_address=front.token_sold_address))
    {% if is_incremental() %}
    AND dt.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
-- Can be removed once dex.trades has block_number & tx_index
INNER JOIN {{transactions}} tx ON tx.block_time=dt.block_time
    AND tx.hash=dt.tx_hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE front.blockchain='{{blockchain}}'
{% if is_incremental() %}
AND front.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

{% endmacro %}
