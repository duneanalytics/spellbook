{%- macro yield_yak_yak_swaps(
        blockchain = null
    )
-%}

{%- set namespace_blockchain = 'yield_yak_' + blockchain -%}

WITH

basic_yak_swaps AS (
    SELECT
        s.contract_address AS yak_router_address
        , s.evt_block_number AS block_number
        , CAST(date_trunc('day', s.evt_block_time) AS date) AS block_date
        , s.evt_block_time AS block_time
        , t.index AS tx_index
        , s.evt_tx_hash AS tx_hash
        , t.to AS tx_to_address
        , t.gas_used * ({% if blockchain == 'arbitrum' %}t.effective_gas_price{% else %}t.gas_price{% endif %} / 1e18) AS tx_fee
        , s.evt_index
        , COUNT(*) OVER (PARTITION BY s.evt_block_number, t.index) AS number_of_swaps_in_tx
        , t."from" AS trader_address
        , s._tokenIn AS swap_token_in_address
        , s._tokenOut AS swap_token_out_address
        , s._amountIn AS swap_amount_in
        , s._amountOut AS swap_amount_out
        -- The line below is needed so that in the next stage we correctly attribute the right YakAdapterSwap logs to the right YakSwap event (YakAdapterSwap events always come before the relevant YakSwap event)
        , LAG(s.evt_index) OVER (PARTITION BY s.evt_tx_hash, s.evt_block_number ORDER BY s.evt_index) AS prev_yak_swap_index_in_tx
    FROM {{ source(namespace_blockchain, 'YakRouter_evt_YakSwap') }} s
    INNER JOIN {{ source(blockchain, 'transactions') }} t
        ON t.hash = s.evt_tx_hash
        AND t.block_number = s.evt_block_number
        AND t.block_date = CAST(date_trunc('day', s.evt_block_time) AS date)
    {%- if is_incremental() %}
        AND {{ incremental_predicate('t.block_time') }}
    WHERE
        {{ incremental_predicate('s.evt_block_time') }}
    {%- endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , s.yak_router_address
    , s.block_number
    , s.block_date
    , s.block_time
    , s.tx_index
    , s.tx_hash
    , s.tx_to_address
    , s.tx_fee
    , s.evt_index
    , s.number_of_swaps_in_tx
    , s.tx_fee / s.number_of_swaps_in_tx AS tx_fee_per_swap
    , s.trader_address
    , s.swap_token_in_address
    , s.swap_token_out_address
    , s.swap_amount_in
    , s.swap_amount_out
    , COUNT(*) AS number_of_hops
    , ARRAY_AGG(
        json_object(
            'adapter_address': CAST(l.contract_address AS varchar),
            'adapter_token_in_address': CAST(varbinary_substring(l.topic1, 13) AS varchar),
            'adapter_token_out_address': CAST(varbinary_substring(l.topic2, 13) AS varchar),
            'adapter_amount_in': varbinary_to_uint256(varbinary_substring(l.data, 1, 32)),
            'adapter_amount_out': varbinary_to_uint256(varbinary_substring(l.data, 33, 32))
        ) ORDER BY l.index
    ) AS yak_adapter_swaps
FROM basic_yak_swaps s
INNER JOIN {{ source(blockchain, 'logs') }} l
    ON l.tx_hash = s.tx_hash
    AND l.block_number = s.block_number
    AND l.index < s.evt_index
    AND (s.prev_yak_swap_index_in_tx IS NULL OR l.index > s.prev_yak_swap_index_in_tx)  -- this line and the one above is what makes sure the relevant YakAdapterSwap logs are paired with the right YakSwap event
    AND l.topic0 = 0xe2bdbc6b7225eb0a972ac943c485a6cc05f7c6811838bce8903f23200fb744fa  -- function signature for YakAdapterSwap(address,address,uint256,uint256)
    {%- if is_incremental() %}
    AND {{ incremental_predicate('l.block_time') }}
    {%- endif %}
GROUP BY
    s.yak_router_address
    , s.block_number
    , s.block_date
    , s.block_time
    , s.tx_index
    , s.tx_hash
    , s.tx_to_address
    , s.tx_fee
    , s.evt_index
    , s.number_of_swaps_in_tx
    , s.tx_fee / s.number_of_swaps_in_tx
    , s.trader_address
    , s.swap_token_in_address
    , s.swap_token_out_address
    , s.swap_amount_in
    , s.swap_amount_out

{%- endmacro -%}