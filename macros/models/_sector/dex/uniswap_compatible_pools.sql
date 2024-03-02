{% macro uniswap_compatible_pools(
    blockchain = null
    , project = null
    , version = null
    , hardcoded_fee = null 
    , fee_column_name = null
    , Factory_evt_PairCreated = null
    , pool_column_name = 'pair'
    , token0_column_name = 'token0'
    , token1_column_name = 'token1'
    )
%}

SELECT 
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , f.{{ pool_column_name }} as pool
    {% if hardcoded_fee %} -- use hardcoded fee if it's exists
    , {{ hardcoded_fee }} as fee
    {% endif %}
    {% if fee_column_name %}
    , f.{{fee_column_name}} as fee -- use fee column if hardcoded fee doesn't exists
    {% endif %}
    {% if not (fee_column_name or hardcoded_fee) %}
    , CAST(NULL as BIGINT) as fee 
    {% endif %}
    , array_agg(
        CAST(ROW(f.{{ token0_column_name }}, f.{{ token1_column_name }}) as ROW(token0 VARBINARY, token1 VARBINARY))
    ) AS tokens
    , 2 AS tokens_in_pool
    , evt_block_time AS creation_block_time
    , evt_block_number AS creation_block_number
    , contract_address
FROM 
{{ Factory_evt_PairCreated }} f
{% if is_incremental() %}
WHERE
{{ incremental_predicate('evt_block_time') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 10

{% endmacro %}