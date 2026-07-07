{% macro native_token_prices(
    blockchain = ''
    , time_unit = 'day'
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
    , date_trunc('{{ time_unit }}', p.minute) as timestamp
    , avg(p.price) as price
FROM
    {{ source('dune', 'blockchains') }} as b
INNER JOIN
    {{ source('prices', 'usd_with_native') }} as p
    ON b.name = p.blockchain
    AND b.token_address = p.contract_address
WHERE
    b.name = '{{blockchain}}'
    {% if is_incremental() -%}
    AND {{ incremental_predicate('p.minute') }}
    {%- elif target.name == 'ci' -%}
    -- bound the CI initial-build scan to recent history so it completes against real data instead of
    -- scanning the full source range; prod and manual runs still get full native price history.
    AND p.minute >= current_date - interval '7' day
    {%- endif %}
GROUP BY
    1, 2, 3, 4, date_trunc('{{ time_unit }}', p.minute)
{%- endmacro %}