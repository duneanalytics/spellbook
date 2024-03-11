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
    , array_agg(
        CAST(ROW(erc20a.symbol, erc20b.symbol) as ROW(token0symbol VARCHAR, token1symbol VARCHAR))
    ) AS token_symbols
    , 2 AS tokens_in_pool
    , f.evt_block_time AS creation_block_time
    , f.evt_block_number AS creation_block_number
    , f.contract_address
FROM 
{{ Factory_evt_PairCreated }} f
LEFT JOIN 
{{ source('tokens', 'erc20') }} erc20a 
    ON f.{{ token0_column_name }} = erc20a.contract_address
    AND erc20a.blockchain = '{{ blockchain }}'
LEFT JOIN 
{{ source('tokens', 'erc20') }} erc20b
    ON f.{{ token1_column_name }} = erc20b.contract_address
    AND erc20b.blockchain = '{{ blockchain }}'
{% if is_incremental() %}
WHERE
{{ incremental_predicate('f.evt_block_time') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 8, 9, 10, 11

{% endmacro %}