{% macro native_token_prices(
    blockchain = ''
    , time_unit = 'hour'
    )
-%}

{%- if blockchain is none or blockchain == '' -%}
    {{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}

SELECT
    b.name as blockchain
    , b.token_address as contract_address
    , b.token_symbol as symbol
    , b.token_decimals as decimals
    , p.timestamp
    , p.price
FROM
    {{ source('dune', 'blockchains') }} as b
INNER JOIN
    {{ source('prices', time_unit) }} as p
    ON b.name = p.blockchain
    AND b.token_address = p.contract_address
WHERE
    b.name = '{{blockchain}}'
    {% if is_incremental() -%}
    AND {{incremental_predicate('p.timestamp')}}
    {%- endif %}
{%- endmacro %}