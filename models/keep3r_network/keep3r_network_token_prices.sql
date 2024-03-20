{{ config(
    schema = 'keep3r_network'
    , alias = 'token_prices'
    , post_hook = '{{ expose_spells(\'["ethereum", "optimism", "polygon"]\',
                                "project", 
                                "keep3r",
                                 \'["0xr3x"]\') }}'
) }}


with dex_tokens as (
    select
        *
    from
    (
        values
        (
            'KP3R',
            'ethereum',
            0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44
        ),
        (
            'WETH',
            'ethereum',
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        ),
        (
            'WMATIC',
            'polygon',
            0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270
        )
    ) as t (label, blockchain, token_address)
),
dex_price as (
    select
        contract_address as token_address,
        blockchain,
        date_trunc('day', hour) as day,
        avg(median_price) as price
    FROM {{ ref('dex_prices') }} prc --on prc.contract_address = tkn.token_address and prc.blockchain = prc.blockchain
    where
        hour >= timestamp '2021-10-10' -- month of $K3PR Mint
        and contract_address in (select token_address from dex_tokens)
    group by 1,2,3--,4
),
dex_trade as (
    select  
        token_address,
        blockchain,
        day,
        avg(price) as price
    from (
        select
            token_sold_address as token_address,
            blockchain,
            block_date as day,
            (amount_usd / token_sold_amount) as price
        from {{ ref('dex_trades') }}
        where 
            token_sold_address in (select token_address from dex_tokens) 
            and block_date >= timestamp '2021-10-10'
        union
        select
            token_bought_address as token_address,
            blockchain,
            block_date as day,
            (amount_usd / token_bought_amount) as price
        from {{ ref('dex_trades') }}
        where 
            token_bought_address in (select token_address from dex_tokens) 
            and block_date >= timestamp '2021-10-10'
        )
    group by 1,2,3
),
prices as (
    select
        COALESCE(prc.token_address,trd.token_address) as token_address,
        COALESCE(prc.day , trd.day) as day,
        COALESCE(prc.price , trd.price) as price
    from
        dex_price prc 
        FULL join dex_trade trd 
        on prc.token_address = trd.token_address 
        and prc.day = trd.day
)

select
    label,
    blockchain,
    tkn.token_address as token_address,
    day,
    price
from
    dex_tokens tkn
    left join prices prc 
    on prc.token_address = tkn.token_address
order by day,token_address
