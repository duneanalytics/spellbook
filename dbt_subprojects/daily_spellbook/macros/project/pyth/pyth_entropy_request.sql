{% macro pyth_entropy_request(blockchain, symbol, entropy_address) %}
SELECT
    '{{ blockchain }}' as blockchain,
    tx_hash,
    cast(value as decimal(38, 0)) / 1e18 as fee,
    '{{ symbol }} 'as symbol,
    substr(input, 17, 20) as provider,
    block_time,
    block_date,
    "from" as caller
FROM
    {{source(blockchain, 'traces')}}
WHERE
    contains(
        array[
            0x93cbf217, -- request(address,bytes32,bool)
            0x19cb825f -- requestWithCallback(address,bytes32)
        ],
        substr(input, 1, 4)
    )
    and tx_success = true
    and to = {{ entropy_address }}
{% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
{% endif %}
{% endmacro %}