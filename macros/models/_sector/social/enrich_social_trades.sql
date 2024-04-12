{% macro enrich_social_trades(base_trades) %}

SELECT 
    t.blockchain AS blockchain
    , CAST(date_trunc('month', t.block_time) AS date) AS block_month
    , t.block_time
    , t.block_number
    , t.project
    , t.trader
    , t.subject
    , t.trade_side
    , t.amount_original
    , t.amount_original * pu.price AS amount_usd
    , t.share_amount
    , t.subject_fee_amount
    , t.subject_fee_amount * pu.price AS subject_fee_amount_usd
    , t.protocol_fee_amount
    , t.protocol_fee_amount * pu.price AS protocol_fee_amount_usd
    , t.currency_contract
    , CASE WHEN t.currency_contract=0x0000000000000000000000000000000000000000 THEN info.native_token_symbol ELSE tok.symbol END AS currency_symbol
    , t.supply
    , t.tx_hash
    , t.evt_index
    , t.contract_address
FROM {{ base_trades }} t
INNER JOIN {{ref('evms_info')}} info
    ON info.blockchain = t.blockchain
LEFT JOIN {{source('tokens', 'erc20')}} tok
    ON tok.blockchain = t.blockchain
    AND tok.contract_address = t.currency_contract
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu
    ON pu.blockchain = t.blockchain
    AND (
        pu.contract_address=info.wrapped_native_token_address
        AND pu.minute = date_trunc('minute', t.block_time)
        )
    {% if is_incremental() %}
    AND
        {{ incremental_predicate('pu.minute') }}
    {% endif %}
{% if is_incremental() %}
WHERE
    {{ incremental_predicate('t.block_time') }}
{% endif %}

{% endmacro %}