{% macro blockchain_token_transfer_metrics(blockchain) %}

SELECT
    '{{ blockchain }}' as blockchain
    ,chain_id
    ,date_trunc('hour',"time") as block_hour
    ,contract_address as token_address
    ,symbol as token_symbol
    ,token_standard as token_standard
    ,avg(price_usd) as price_usd
    ,sum(amount_raw) as transfer_volume_raw
    ,sum(amount) as transfer_volume
    ,sum(amount_usd) as transfer_volume_usd
    ,count(*) as transfer_count
FROM {{source('tokens_' ~ blockchain, 'transfers')}}
LEFT JOIN {{ source('evms','info') }} evm
    ON evm.blockchain = '{{ blockchain }}'
WHERE token_standard != 'native'    --temporary until we fix null address in native transfers
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
{% else %}
AND {{ incremental_predicate('block_time') }}   -- temp for fast CI
{% endif %}
GROUP BY 1,2,3,4,5,6
{% endmacro %}
