{% macro transfers_enrich(
    base_transfers = null
    , blockchain = null
    , tokens_erc20_model = source('tokens', 'erc20')
    , prices_interval = 'hour'
    , trusted_tokens_model = source('prices', 'trusted_tokens')
    , transfers_start_date = '2000-01-01'
    , usd_amount_threshold = 1000000000
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
    {% if is_incremental() or true %}
    WHERE
        {{ incremental_predicate('block_date') }}
    {% else %}
    WHERE
        block_date >= TIMESTAMP '{{ transfers_start_date }}'
    {% endif %}
)
, temp_prices as (
    --temp: until prices_coinpaprika.hour is fixed, use raw SQL under the view
    with prices_tokens_patched as (
        select
            token_id
            ,blockchain
            ,contract_address
            ,symbol
            ,decimals
        from
            prices.tokens
        where blockchain is not null
        and contract_address is not null    

        union all
        
        select 
            t.token_id
            ,d.name as blockchain
            ,d.token_address as contract_address
            ,d.token_symbol as symbol
            ,d.token_decimals as decimals
        from prices.tokens t
        inner join dune.blockchains d
            on d.token_symbol = t.symbol
            and t.blockchain is null
            and t.contract_address is null
    )
    select
        timestamp
        ,blockchain
        ,contract_address
        ,coalesce(erc20.symbol, prices_tokens.symbol) as symbol
        ,price
        ,coalesce(erc20.decimals, prices_tokens.decimals) as decimals
        ,volume
        , case
            -- only two known chains which cast address in prices.tokens
            when blockchain = 'solana' then to_base58(contract_address)
            when blockchain = 'tron' then to_tron_address(contract_address)
            else cast(contract_address as varchar)
        end as contract_address_varchar
    from prices_v2_coinpaprika.hour
    left join delta_prod.tokens.erc20 as erc20
        using (blockchain, contract_address)
    left join prices_tokens_patched as prices_tokens  -- fallback if not in erc20 tokenset
        using (blockchain, contract_address)
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
        temp_prices
        --{{ source('prices_coinpaprika', prices_interval) }}
    {% if is_incremental() or true %}
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
        ON date_trunc('{{ prices_interval }}', t.block_time) = prices.timestamp
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