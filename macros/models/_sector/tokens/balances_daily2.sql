{%- macro balances_daily_enriched(balances_daily_agg_base, start_date, native_token='ETH') %}

with forward_fill as (
    select
        blockchain
        ,d.day as day
        ,address
        ,token_address
        ,token_standard
        ,token_id
        ,balance_raw
        from {{ref('tokens_days')}} d
        left join {{balances_daily_agg_base}} b
            ON  d.day >= b.day
            and (b.next_update_day is null OR d.day < b.next_update_day) -- perform forward fill
            and b.day < date(date_trunc('day',now()))
        where d.day >= cast('{{start_date}}' as date)
)

select
    b.blockchain,
    b.day,
    b.address,
    b.token_address,
    b.token_standard,
    b.balance_raw,
    CASE
        WHEN b.token_standard = 'erc20' THEN b.balance_raw / power(10, erc20_tokens.decimals)
        WHEN b.token_standard = 'native' THEN b.balance_raw / power(10, 18)
        ELSE b.balance_raw
    END as balance,
    erc20_tokens.symbol as token_symbol,
    token_id,
    nft_tokens.name as collection_name,
    CASE
        WHEN b.token_standard = 'erc20' THEN (b.balance_raw / power(10, erc20_tokens.decimals)) * p.price
        WHEN b.token_standard = 'native' THEN (b.balance_raw / power(10, 18)) * p.price
        ELSE b.balance_raw
    END as balance_usd
from(
    select * from forward_fill
    where balance_raw > 0
    ) b
left join {{ ref('tokens_nft') }} nft_tokens on (
   nft_tokens.blockchain = 'ethereum'
   AND nft_tokens.contract_address = b.token_address
   AND b.token_standard in ('erc721', 'erc1155')
   )
left join {{ref('prices_usd_daily')}} p
    on (token_standard = 'erc20'
    and b.blockchain = 'ethereum'
    and b.token_address = p.contract_address
    and b.day = p.day)
    or (token_standard = 'native'
    and p.blockchain is null
    and p.contract_address is null
    and p.symbol = '{{native_token}}'
    and b.day = p.day)
left join {{ source('tokens', 'erc20') }} erc20_tokens on
    erc20_tokens.blockchain = 'ethereum'
    AND erc20_tokens.contract_address = b.token_address
    AND b.token_standard = 'erc20'

{% endmacro %}
