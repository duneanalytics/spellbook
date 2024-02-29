{% macro uniswap_compatible_pools(
    blockchain = null
    , project = null
    , version = null
    , Factory_evt_PairCreated = null
    , fee = null 
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
    , COALESCE({{fee}}, f.fee) as fee
    , array_agg(
        CAST(ROW(f.{{ token0_column_name }}, f.{{ token1_column_name }}) AS ROW(f.{{ token0_column_name }} VARBINARY, f.{{ token1_column_name }} VARBINARY))
    ) AS tokens 
    , 2 AS tokens_in_pool
    , evt_block_time AS creation_block_time
    , evt_block_number AS creation_block_number
    , contract_address
FROM 
{{ Factory_evt_PairCreated }} f
{% if is_incremental() %}
WHERE
{{ incremental_predicate('f.evt_block_time') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 7, 8, 9, 10

{% endmacro %}