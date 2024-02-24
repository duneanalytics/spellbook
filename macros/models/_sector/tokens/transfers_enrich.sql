{%- macro case_when_token_standard(native_column, erc20_column, else_column) -%}
CASE token_standard
    WHEN 'native' THEN {{native_column}}
    WHEN 'erc20' THEN {{erc20_column}}
    WHEN 'bep20' THEN {{erc20_column}}
    ELSE {{else_column}}
END
{%- endmacro-%}

{% macro transfers_enrich(
    base_transfers = null
    , tokens_erc20_model = null
    , prices_model = null
    , evms_info_model = null
    , transfers_start_date = '2000-01-01'
    , blockchain = null
    )
%}

WITH base_transfers as (
    SELECT
        *
    FROM
        {{ base_transfers }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_date') }}
    {% endif %}
)
, prices AS (
    SELECT
        minute
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    FROM
        {{ prices_model }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('minute') }}
    {% else %}
    WHERE
        minute >= TIMESTAMP '{{ transfers_start_date }}'
    {% endif %}
)
SELECT
    t.unique_key
    , t.blockchain
    , t.block_date
    , t.block_time
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
    , {{case_when_token_standard('evms_info.native_token_symbol', 'tokens_erc20.symbol', 'NULL')}} AS symbol
    , t.amount_raw
    , {{case_when_token_standard('t.amount_raw / power(10, 18)', 't.amount_raw / power(10, tokens_erc20.decimals)', 'cast(t.amount_raw as double)')}} AS amount
    , prices.price AS price_usd
    , {{case_when_token_standard('(t.amount_raw / power(10, 18)) * prices.price',
        '(t.amount_raw / power(10, tokens_erc20.decimals)) * prices.price',
        'NULL')}} AS amount_usd
FROM
    base_transfers as t
INNER JOIN
    {{ evms_info_model }} as evms_info
    ON evms_info.blockchain = t.blockchain
LEFT JOIN
    {{ tokens_erc20_model }} as tokens_erc20
    ON tokens_erc20.blockchain = t.blockchain
    AND tokens_erc20.contract_address = t.contract_address
LEFT JOIN
    prices
    ON date_trunc('minute', t.block_time) = prices.minute
    AND CASE
        WHEN t.token_standard = 'native'
            THEN
            prices.blockchain IS NULL
            AND prices.contract_address IS NULL
            AND evms_info.native_token_symbol = prices.symbol
        ELSE
            prices.blockchain = '{{ blockchain }}'
            AND t.contract_address = prices.contract_address
        END
{%- endmacro %}