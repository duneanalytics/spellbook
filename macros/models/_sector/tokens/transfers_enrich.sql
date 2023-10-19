{%- macro transfers_enrich(blockchain, transfers_base) %}
SELECT t.block_time
, t.block_number
, t.tx_hash
, t.evt_index
, t.trace_address
, t.token_standard
, t.tx_from
, t.tx_to
, t.tx_index
, t."from"
, t.to
, t.contract_address
,CASE WHEN t.token_standard = 'native' THEN 'SYMBOL' -- this if should be a helper macro
    WHEN t.token_standard = 'erc20' OR t.token_standard = 'bep20' THEN tokens_erc20.symbol
    ELSE NULL
 END AS symbol
, t.amount_raw
, CASE WHEN t.token_standard = 'native' THEN t.amount_raw / power(10, 18)
    WHEN t.token_standard = 'erc20' OR THEN t.token_standard = 'bep20'  t.amount_raw / power(10, tokens_erc20.decimals)
    ELSE cast(t.amount_raw as double)
 END AS amount
, prices.price AS usd_price
, CASE WHEN t.token_standard = 'native' THEN (t.amount_raw / power(10, 18)) * prices.price
    WHEN t.token_standard = 'erc20' OR t.token_standard = 'bep20' THEN (t.amount_raw / power(10, tokens_erc20.decimals)) * prices.price
    ELSE NULL
 END AS usd_amount
FROM {{transfers_base}} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} prices ON prices.blockchain = '{{blockchain}}'
LEFT JOIN {{ref('tokens_erc20')}} tokens_erc20 on tokens_erc20.blockchain = '{{blockchain}}' AND tokens_erc20.contract_address = t.contract_address
    AND (
            prices.contract_address=t.contract_address
            OR t.contract_address IS NULL AND prices.contract_address=(SELECT wrapped_native_token_address FROM {{ ref('evms_info') }} WHERE blockchain='{{blockchain}}')
        )
    AND prices.minute = date_trunc('minute', t.block_time)
{%- endmacro %}