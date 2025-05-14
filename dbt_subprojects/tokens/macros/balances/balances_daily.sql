{%- macro balances_daily(balances_daily_agg, start_date, native_token='ETH') %}

with changed_balances as (
    select
    blockchain
    ,day
    ,address
    ,token_symbol
    ,token_address
    ,token_standard
    ,token_id
    ,balance
    ,lead(cast(day as timestamp)) over (partition by token_address,address,token_id order by day asc) as next_update_day
    from {{balances_daily_agg}}
)

,days as (
    select *
    from unnest(
         sequence(cast('{{start_date}}' as date)
                , date(date_trunc('day',now()))
                , interval '1' day
                )
         ) as foo(day)
)

, forward_fill as (
    select
        blockchain
        ,cast(d.day as timestamp) as day
        ,address
        ,token_symbol
        ,token_address
        ,token_standard
        ,token_id
        ,balance
        from days d
        left join changed_balances b
            ON  d.day >= b.day
            and (b.next_update_day is null OR d.day < b.next_update_day) -- perform forward fill
)

, filtered_forward_fill as (
    select * from forward_fill
    where balance > 0
)

-- New CTE to fetch only relevant prices from the daily aggregated source table
, prices_for_join AS (
    SELECT
        pd.day AS price_day,
        pd.price AS price_value, -- This is the daily average price from prices.usd_daily
        pd.blockchain AS price_blockchain,
        pd.symbol AS price_symbol,
        pd.contract_address AS price_contract_address
    FROM
        {{ source('prices', 'usd_daily') }} AS pd -- Using source instead of ref
    INNER JOIN -- This join ensures we only pull prices for relevant days and tokens
        filtered_forward_fill fff ON pd.day = fff.day -- Match on day
            AND (
                -- Condition for ERC20 tokens
                (fff.token_standard = 'erc20' AND pd.contract_address = fff.token_address AND pd.blockchain = fff.blockchain)
                OR
                -- Condition for NATIVE tokens
                -- (Assuming prices_usd_daily carries over the null blockchain/contract_address for native tokens,
                -- or handles native token representation consistently with how 'prices.usd' did)
                (fff.token_standard = 'native' AND pd.symbol = upper('{{native_token}}') AND pd.contract_address IS NULL AND pd.blockchain IS NULL)
            )
)

-- Final SELECT statement
select
    b.*
    ,b.balance * p.price_value as balance_usd
from filtered_forward_fill b
left join prices_for_join p
    ON b.day = p.price_day -- Join on day
    -- And ensure the correct token is matched based on its type
    AND (
        (b.token_standard = 'erc20' AND b.token_address = p.price_contract_address AND b.blockchain = p.price_blockchain)
        OR
        (b.token_standard = 'native' AND p.price_symbol = upper('{{native_token}}') AND p.price_contract_address IS NULL AND p.price_blockchain IS NULL)
    )

{% endmacro %}
