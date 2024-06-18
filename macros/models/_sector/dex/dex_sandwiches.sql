{% macro dex_sandwiches(blockchain, transactions) %}

-- Checking that each frontrun trade has a matching backrun and at least one victim in between
WITH indexed_sandwich_trades AS (
    SELECT DISTINCT front.block_time
    , t.tx_hash_all AS tx_hash
    , front.project
    , front.version
    , front.project_contract_address
    , t.evt_index_all AS evt_index
    FROM {{ ref('dex_solana_trades') }} front
    INNER JOIN {{ ref('dex_solana_trades') }} back ON back.blockchain='{{blockchain}}'
        AND front.block_time=back.block_time
        AND front.project_contract_address=back.project_contract_address
        AND front.tx_from=back.tx_from
        AND front.tx_hash!=back.tx_hash
        AND front.token_sold_mint_address=back.token_bought_mint_address
        AND front.token_bought_mint_address=back.token_sold_mint_address
        AND front.evt_index + 1 < back.evt_index
        {% if is_incremental() %}
        AND {{ incremental_predicate('back.block_time') }}
        {% endif %}
    INNER JOIN {{ ref('dex_solana_trades') }} victim ON victim.blockchain='{{blockchain}}'
        AND front.block_time=victim.block_time
        AND front.project_contract_address=victim.project_contract_address
        AND front.tx_from!=victim.tx_from
        AND front.token_bought_mint_address=victim.token_bought_mint_address
        AND front.token_sold_mint_address=victim.token_sold_mint_address
        AND victim.evt_index BETWEEN front.evt_index AND back.evt_index
        {% if is_incremental() %}
        AND {{ incremental_predicate('victim.block_time') }}
        {% endif %}
    CROSS JOIN UNNEST(ARRAY[(front.tx_hash, front.evt_index), (back.tx_hash, back.evt_index)]) AS t(tx_hash_all, evt_index_all)
    WHERE front.blockchain='{{blockchain}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('front.block_time') }}
    {% endif %}
    )

-- Joining back with dex.trades to get the rest of the data & adding block_number and tx_index to the mix
SELECT dt.blockchain
, dt.project
, dt.version
, dt.block_time
, dt.block_month
, tx.block_number
, dt.token_sold_mint_address
, dt.token_bought_mint_address
, dt.token_sold_vault
, dt.token_bought_vault
, dt.token_sold_symbol
, dt.token_bought_symbol
, dt.trader_id
, dt.tx_id
, dt.project_contract_address
, dt.token_pair
, tx.index AS tx_index
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
FROM {{ ref('dex_solana_trades') }} dt
INNER JOIN indexed_sandwich_trades s ON dt.block_time=s.block_time
    AND dt.tx_hash=s.tx_hash
    AND dt.project_contract_address=s.project_contract_address
    AND dt.evt_index=s.evt_index
-- Adding block_number and tx_index to the mix, can be removed once those are in dex.trades
INNER JOIN {{transactions}} tx ON tx.block_time=s.block_time
    AND tx.hash=s.tx_hash
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% endif %}
WHERE dt.blockchain='{{blockchain}}'
{% if is_incremental() %}
AND {{ incremental_predicate('dt.block_time') }}
{% endif %}

{% endmacro %}