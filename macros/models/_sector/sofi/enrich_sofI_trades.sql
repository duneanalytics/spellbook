{% macro enrich_sofi_trades(blockchain, base_trades_models, raw_transactions) %}


{% for base_trades_model in base_trades_models %}
SELECT t.blockchain
, date_trunc('month', block_time) AS block_month
, block_time
, t.block_number
, t.project
, trader
, subject
, txs."from" AS tx_from
, txs.to AS tx_to
, t.trade_side
, t.amount
, t.amount*pu.price AS amount_usd
, t.share_amount
, t.subject_amount
, t.subject_amount*pu.price AS subject_amount_usd
, t.protocol_amount
, t.protocol_amount*pu.price AS protocol_amount_usd
, t.currency_contract
, t.currency_symbol
, supply
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{ base_trades_model }} t
INNER JOIN {{raw_transactions}} txs ON txs.block_number=t.block_number
    AND  txs.hash=t.tx_hash
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN {{ref('evms_info')}} info ON info.blockchain='{{ blockchain }}'
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = '{{ blockchain }}'
    AND (pu.contract_address=info.wrapped_native_token_address
    AND pu.minute = date_trunc('minute', t.block_time)
        )
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}

{% endmacro %}