{% macro transfers_enrich(
    base_transfers = null
    , tokens_erc20_model = source('tokens', 'erc20')
    , prices_model = 'day'
    , trusted_tokens_model = source('prices', 'trusted_tokens')
    , transfers_start_date = '2000-01-01'
    , blockchain = null
    , usd_amount_threshold = 100000000
    )
%}

{%- if blockchain is none or blockchain == '' -%}
    {{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}
{%- if base_transfers is none or base_transfers == '' -%}
    {{ exceptions.raise_compiler_error("base_transfers parameter cannot be null or empty") }}
{%- endif -%}

WITH base_transfers as (
    SELECT
        *
    FROM
        {{ base_transfers }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_date') }}
    {% else %}
    WHERE
        block_date >= TIMESTAMP '{{ transfers_start_date }}'
    {% endif %}
)
, prices AS (
    SELECT
        timestamp
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    FROM
        {{ source('prices', prices_model) }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('timestamp') }}
    {% else %}
    WHERE
        timestamp >= TIMESTAMP '{{ transfers_start_date }}'
    {% endif %}
)
, trusted_tokens AS (
    SELECT
        blockchain
        , contract_address
    FROM {{ trusted_tokens_model }}
)
, transfers as (
    SELECT
        t.unique_key
        , t.blockchain
        , t.block_month
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
        , coalesce(tokens_erc20.symbol, prices.symbol) AS symbol
        , t.amount_raw
        , t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) AS amount
        , prices.price AS price_usd
        , t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) * prices.price AS amount_usd
        , CASE WHEN trusted_tokens.blockchain IS NOT NULL THEN true ELSE false END AS is_trusted_token
    FROM
        base_transfers as t
    LEFT JOIN
        {{ tokens_erc20_model }} as tokens_erc20
        ON tokens_erc20.blockchain = t.blockchain
        AND tokens_erc20.contract_address = t.contract_address
    LEFT JOIN
        trusted_tokens
        ON trusted_tokens.blockchain = t.blockchain
        AND trusted_tokens.contract_address = t.contract_address
    LEFT JOIN
        prices
        ON date_trunc('{{ prices_model }}', t.block_time) = prices.timestamp
        AND t.blockchain = prices.blockchain
        AND t.contract_address = prices.contract_address
)
, final as (
    SELECT
        unique_key
        , blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , to
        , contract_address
        , symbol
        , amount_raw
        , amount
        , price_usd
        , CASE
            WHEN is_trusted_token = true THEN amount_usd
            WHEN (is_trusted_token = false AND amount_usd < {{ usd_amount_threshold }}) THEN amount_usd
            WHEN (is_trusted_token = false AND amount_usd >= {{ usd_amount_threshold }}) THEN CAST(NULL as double) /* ignore inflated outlier prices */
            END AS amount_usd
    FROM
        transfers
)
SELECT
    *
FROM
    final
{%- endmacro %}