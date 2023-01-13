{{ config(alias='price_improvement_data',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha", "yosojo"]\') }}'
)}}

with batches_with_nested_uids_and_trades as (
    select
        call_tx_hash,
        collect_list(orderUid) as uids,
        trades
    -- Replace with sources
    FROM (SELECT * FROM gnosis_protocol_v2_ethereum.GPv2Settlement_evt_Trade ORDER BY evt_index ASC)
    JOIN gnosis_protocol_v2_ethereum.GPv2Settlement_call_settle ON call_tx_hash = evt_tx_hash
    GROUP BY call_tx_hash, trades
),

uid_to_limit_prices (
    select
        call_tx_hash as tx_hash,
        u.uid,
        get_json_object(trades[pos], '$.sellAmount') as sell_amount_limit,
        get_json_object(trades[pos], '$.buyAmount') as buy_amount_limit,
        get_json_object(trades[pos], '$.flags')::integer & 1 as order_kind
    from batches_with_nested_uids_and_trades
    lateral view posexplode(uids) u as pos, uid
),

data as (
    select
    case when order_kind = 1 then 'BUY' else 'SELL' end as order_kind,
    buy_amount_limit,
    CASE when order_kind = 0 THEN (buy_amount_limit / (1.0 - (slippage_bips / 10000.0))) else (buy_amount_limit) end as buy_quote,
    atoms_bought,
    atoms_bought / pow(10, A.decimals) as units_bought,
    buy_price,
    A.decimals as buy_decimals,
    sell_amount_limit,
    CASE when order_kind = 1 THEN (sell_amount_limit / (1.0 + (slippage_bips / 10000.0))) else (sell_amount_limit) end as sell_quote,
    atoms_sold,
    atoms_sold / pow(10, B.decimals) as units_sold,
    sell_price,
    B.decimals as sell_decimals,
    usd_value,
    slippage_bips,
    (1.0 - (slippage_bips / 10000.0)) as buy_tolerance,
    (1.0 + (slippage_bips / 10000.0)) as sell_tolerance,
    order_uid
from cow_protocol_ethereum.app_data as ad
inner join cow_protocol_ethereum.trades as t on t.app_data = ad.app_hash
INNER JOIN uid_to_limit_prices AS lp ON t.order_uid=lp.uid
LEFT JOIN (select * from tokens.erc20 where blockchain = 'ethereum') A ON A.contract_address=t.buy_token_address
LEFT JOIN (select * from tokens.erc20 where blockchain = 'ethereum') B ON B.contract_address=t.sell_token_address
where slippage_bips is not null
),

results as (
    select *,
        CASE
            WHEN order_kind = 'SELL' THEN (atoms_bought - (buy_amount_limit / buy_tolerance))
            ELSE ((sell_amount_limit / sell_tolerance) - atoms_sold)
        END AS surplus_atoms,
    100.0 * (CASE
        WHEN order_kind = 'SELL'
            THEN (((atoms_bought - (buy_amount_limit / buy_tolerance)) / (buy_amount_limit / buy_tolerance)))
            ELSE (((sell_amount_limit / sell_tolerance) - (atoms_sold)) / (sell_amount_limit / sell_tolerance))
    END) AS surplus_percentage,
    CASE
        WHEN order_kind = 'SELL'
        THEN (atoms_bought - buy_quote) / POWER(10, buy_decimals) * (usd_value / units_bought)
        ELSE ((sell_amount_limit / sell_tolerance) - (atoms_sold)) * (usd_value / (sell_amount_limit / sell_tolerance))
      END AS surplus_usd
    from data
)

select
    *
from results
