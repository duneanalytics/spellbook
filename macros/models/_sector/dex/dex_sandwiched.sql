{% macro dex_sandwiched(blockchain, transactions, sandwiches) %}

WITH sandwich_bounds AS (
    SELECT front.block_time
    , front.tx_id AS min_tx_id
    , back.tx_id AS max_tx_id
    , front.project_contract_address    
    , front.token_bought_mint_address
    , front.token_sold_mint_address
    FROM {{sandwiches}} front
    INNER JOIN {{sandwiches}} back ON front.block_time=back.block_time
        AND front.trader_id=back.trader_id
        --AND front.project_contract_address=back.project_contract_address
        AND front.token_sold_mint_address=back.token_bought_mint_address
        AND front.token_bought_mint_address=back.token_sold_mint_address
        AND front.tx_id+1 < back.tx_id
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
, txs.block_slot
, dt.block_month
, dt.token_sold_mint_address
, dt.token_bought_mint_address
, dt.token_sold_vault
, dt.token_bought_vault
, dt.token_sold_symbol
, dt.token_bought_symbol
, dt.trader_id
, dt.tx_id
--, dt.project_contract_address
, dt.token_pair
, dt.token_sold_amount_raw
, dt.token_bought_amount_raw
, dt.token_sold_amount
, dt.token_bought_amount
, dt.amount_usd
, dt.fee_tier
, dt.fee_usd
, dt.project_program_id
, dt.outer_instruction_index
, dt.inner_instruction_index
, dt.trade_source
, dt.evt_index
, txs.index AS tx_index
FROM {{ ref('dex_solana_trades') }} dt
INNER JOIN sandwich_bounds sb ON sb.block_time=dt.block_time
    --AND sb.project_contract_address=dt.project_contract_address
    AND sb.token_bought_mint_address=dt.token_bought_mint_address
    AND sb.token_sold_mint_address=dt.token_sold_mint_address
    AND dt.tx_id BETWEEN sb.min_tx_id AND sb.max_tx_id
INNER JOIN {{transactions}} txs ON txs.block_time=dt.block_time
    AND txs.id=dt.tx_id
    {% if is_incremental() %}
    AND {{ incremental_predicate('txs.block_time') }}
    {% endif %}
WHERE dt.blockchain='{{blockchain}}'
{% if is_incremental() %}
AND {{ incremental_predicate('dt.block_time') }}
{% endif %}

{% endmacro %}