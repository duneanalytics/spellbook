{{ config(alias='trade_slippage',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha", "josojo"]\') }}'
)}}

with 
raw_data as (
    select
    block_number,
    block_time,
    case when (flags % 2) = 0 then 'SELL' else 'BUY' end  as order_type,
    limit_buy_amount,
    CASE when (flags % 2) = 0 THEN (limit_buy_amount / (1.0 - (slippage_bips / 10000.0))) else (limit_buy_amount) end as buy_quote,
    atoms_bought,
    atoms_bought / pow(10, A.decimals) as units_bought,
    buy_price,
    A.decimals as buy_decimals,
    limit_sell_amount,
    CASE when (flags % 2) = 1 THEN (limit_sell_amount / (1.0 + (slippage_bips / 10000.0))) else (limit_sell_amount) end as sell_quote,
    atoms_sold,
    atoms_sold / pow(10, B.decimals) as units_sold,
    sell_price,
    B.decimals as sell_decimals,
    usd_value,
    slippage_bips as slippage_tolerance_bips,
    (1.0 - (slippage_bips / 10000.0)) as buy_tolerance,
    (1.0 + (slippage_bips / 10000.0)) as sell_tolerance,
    order_uid
from {{ref('cow_protocol_ethereum_app_data')}} as ad
inner join {{ ref('cow_protocol_ethereum_trades') }} as t on t.app_data = ad.app_hash
left join (select * from {{ ref('tokens_erc20') }} where blockchain = 'ethereum') A ON A.contract_address=t.buy_token_address
left join (select * from {{ ref('tokens_erc20') }} where blockchain = 'ethereum') B ON B.contract_address=t.sell_token_address
where slippage_bips is not null
),

results as (
    select order_uid, block_number, block_time, buy_quote, sell_quote, slippage_tolerance_bips, usd_value,
        CASE
            WHEN order_type = 'SELL' THEN (atoms_bought - buy_quote)
            ELSE (sell_quote - atoms_sold)
        END AS slippage_atoms,
    100.0 * (CASE
        WHEN order_type = 'SELL'
            THEN (((atoms_bought - buy_quote) / buy_quote))
            ELSE ((sell_quote - atoms_sold) / sell_quote)
    END) AS slippage_percentage,
    CASE
        WHEN order_type = 'SELL'
        THEN (atoms_bought - buy_quote) * (usd_value / atoms_bought)
        ELSE (sell_quote - atoms_sold) * (usd_value / atoms_sold)
      END AS slippage_usd
    from raw_data
)
select * from results
