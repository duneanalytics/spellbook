{{ config(alias='trade_slippage',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha", "josojo"]\') }}'
)}}

with 
raw_data as (
    select
    order_uid,
    block_number,
    block_time,
    case when (flags % 2) = 0 then 'SELL' else 'BUY' end  as order_type,
    CASE when (flags % 2) = 0 THEN (limit_buy_amount / (1.0 - (slippage_bips / 10000.0))) else (limit_buy_amount) end as buy_quote,
    atoms_bought,
    CASE when (flags % 2) = 1 THEN (limit_sell_amount / (1.0 + (slippage_bips / 10000.0))) else (limit_sell_amount) end as sell_quote,
    atoms_sold,
    usd_value as trade_usd_value,
    slippage_bips as tolerance_bips
from {{ref('cow_protocol_ethereum_app_data')}} as ad
inner join {{ ref('cow_protocol_ethereum_trades') }} as t on t.app_data = ad.app_hash
left join (select * from {{ ref('tokens_erc20') }} where blockchain = 'ethereum') A ON A.contract_address=t.buy_token_address
left join (select * from {{ ref('tokens_erc20') }} where blockchain = 'ethereum') B ON B.contract_address=t.sell_token_address
where slippage_bips is not null
),

results as (
    select order_uid,
           block_number,
           block_time,
           buy_quote,
           sell_quote,
           tolerance_bips,
           trade_usd_value,
           CASE
               WHEN order_type = 'SELL' THEN (atoms_bought - buy_quote)
               ELSE (sell_quote - atoms_sold)
               END  AS amount_atoms,
           100.0 * (CASE
                        WHEN order_type = 'SELL'
                            THEN ((atoms_bought - buy_quote) / buy_quote)
                        ELSE ((sell_quote - atoms_sold) / sell_quote)
               END) AS amount_percentage,
           CASE
               WHEN order_type = 'SELL'
                   THEN (atoms_bought - buy_quote) * (trade_usd_value / atoms_bought)
               ELSE (sell_quote - atoms_sold) * (trade_usd_value / atoms_sold)
               END  AS amount_usd
    from raw_data
)
select * from results
