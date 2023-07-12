{{  config(
        alias='trades',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash', 'order_uid', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha"]\') }}'
    )
}}

-- Find the PoC Query here: https://dune.com/queries/2360196
WITH
-- First subquery joins buy and sell token prices from prices.usd
-- Also deducts fee from sell amount
trades_with_prices AS (
    SELECT try_cast(date_trunc('day', evt_block_time) as date) as block_date,
           evt_block_number          as block_number,
           evt_block_time            as block_time,
           evt_tx_hash               as tx_hash,
           evt_index,
           trade.contract_address    as project_contract_address,
           owner                     as trader,
           orderUid                  as order_uid,
           sellToken                 as sell_token,
           buyToken                  as buy_token,
           (sellAmount - feeAmount)  as sell_amount,
           buyAmount                 as buy_amount,
           feeAmount                 as fee_amount,
           ps.price                  as sell_price,
           pb.price                  as buy_price
    FROM {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_evt_Trade') }} trade
             LEFT OUTER JOIN {{ source('prices', 'usd') }} as ps
                             ON sellToken = ps.contract_address
                                 AND ps.minute = date_trunc('minute', evt_block_time)
                                 AND ps.blockchain = 'ethereum'
                                 {% if is_incremental() %}
                                 AND ps.minute >= date_trunc("day", now() - interval '1 week')
                                 {% endif %}
             LEFT OUTER JOIN {{ source('prices', 'usd') }} as pb
                             ON pb.contract_address = (
                                 CASE
                                     WHEN buyToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                     ELSE buyToken
                                     END)
                                 AND pb.minute = date_trunc('minute', evt_block_time)
                                 AND pb.blockchain = 'ethereum'
                                 {% if is_incremental() %}
                                 AND pb.minute >= date_trunc("day", now() - interval '1 week')
                                 {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
-- Second subquery gets token symbol and decimals from tokens.erc20 (to display units bought and sold)
trades_with_token_units as (
    SELECT block_date,
           block_number,
           block_time,
           tx_hash,
           evt_index,
           project_contract_address,
           order_uid,
           trader,
           sell_token                        as sell_token_address,
           (CASE
                WHEN ts.symbol IS NULL THEN sell_token
                ELSE ts.symbol
               END)                          as sell_token,
           buy_token                         as buy_token_address,
           (CASE
                WHEN tb.symbol IS NULL THEN buy_token
                WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'ETH'
                ELSE tb.symbol
               END)                          as buy_token,
           sell_amount / pow(10, ts.decimals) as units_sold,
           sell_amount                       as atoms_sold,
           buy_amount / pow(10, tb.decimals)  as units_bought,
           buy_amount                        as atoms_bought,
           -- We use sell value when possible and buy value when not
           fee_amount / pow(10, ts.decimals)  as fee,
           fee_amount                        as fee_atoms,
           sell_price,
           buy_price
    FROM trades_with_prices
             LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} ts
                             ON ts.contract_address = sell_token
             LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tb
                             ON tb.contract_address =
                                (CASE
                                     WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                                     ELSE buy_token
                                    END)
),
-- This, independent, aggregation defines a mapping of order_uid and trade
sorted_orders as (
    select
        evt_tx_hash,
        evt_block_number,
        collect_list(orderUid) as order_ids
    from (
        select
            evt_tx_hash,
            evt_block_number,
            orderUid
        from gnosis_protocol_v2_ethereum.GPv2Settlement_evt_Trade
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        distribute by
            evt_tx_hash, evt_block_number
        sort by
            evt_index
    )
    group by evt_tx_hash, evt_block_number
),

orders_and_trades as (
    select
        evt_tx_hash,
        trades,
        order_ids
    from sorted_orders
    inner join {{ source('gnosis_protocol_v2_ethereum', 'GPv2Settlement_call_settle') }}
        on evt_block_number = call_block_number
        and evt_tx_hash = call_tx_hash
-- this is implied by the inner join
--      and call_success = true
),
-- Validate Uid <--> app_data mapping here: https://dune.com/queries/1759039?d=1
uid_to_app_id as (
    select
        distinct uid,
        get_json_object(trade, '$.appData') as app_data,
        get_json_object(trade, '$.receiver') as receiver,
        get_json_object(trade, '$.sellAmount') as limit_sell_amount,
        get_json_object(trade, '$.buyAmount') as limit_buy_amount,
        get_json_object(trade, '$.validTo') as valid_to,
        get_json_object(trade, '$.flags') as flags
    from orders_and_trades
        lateral view posexplode(order_ids) o as i, uid
        lateral view posexplode(trades) t as j, trade
    where i = j
),

eth_flow_senders as (
    select
        sender,
        concat(output_orderHash, substring(event.contract_address, 3, 40), 'ffffffff') as order_uid
    from {{ source('cow_protocol_ethereum', 'CoWSwapEthFlow_evt_OrderPlacement') }} event
    inner join {{ source('cow_protocol_ethereum', 'CoWSwapEthFlow_call_createOrder') }} call
        on call_block_number = evt_block_number
        and call_tx_hash = evt_tx_hash
        and call_success = true
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),


valued_trades as (
    SELECT block_date,
           block_number,
           block_time,
           tx_hash,
           evt_index,
           CAST(ARRAY() as array<bigint>) AS trace_address,
           project_contract_address,
           trades.order_uid,
           -- ETH Flow orders have trader = sender of orderCreation.
           case when sender is not null then sender else trader end as trader,
           sell_token_address,
           case when sender is not null then 'ETH' else sell_token end as sell_token,
           buy_token_address,
           buy_token,
           case
                 when lower(buy_token) > lower(sell_token) then concat(sell_token, '-', buy_token)
                 else concat(buy_token, '-', sell_token)
               end as token_pair,
           units_sold,
           CAST(atoms_sold AS DECIMAL(38,0)) AS atoms_sold,
           units_bought,
           CAST(atoms_bought AS DECIMAL(38,0)) AS atoms_bought,
           (CASE
                WHEN sell_price IS NOT NULL THEN
                    -- Choose the larger of two prices when both not null.
                    CASE
                        WHEN buy_price IS NOT NULL and buy_price * units_bought > sell_price * units_sold
                            then buy_price * units_bought
                        ELSE sell_price * units_sold
                        END
                WHEN sell_price IS NULL AND buy_price IS NOT NULL THEN buy_price * units_bought
                ELSE NULL::numeric
               END)                                        as usd_value,
           buy_price,
           buy_price * units_bought                        as buy_value_usd,
           sell_price,
           sell_price * units_sold                         as sell_value_usd,
           fee,
           fee_atoms,
           (CASE
                WHEN sell_price IS NOT NULL THEN sell_price * fee
                WHEN buy_price IS NOT NULL THEN buy_price * units_bought * fee / units_sold
                ELSE NULL::numeric
           END)                                            as fee_usd,
           app_data,
           case
              when receiver = '0x0000000000000000000000000000000000000000'
              then trader
              else receiver
           end                                    as receiver,
           limit_sell_amount,
           limit_buy_amount,
           valid_to,
           flags,
           case when (flags % 2) = 0 then 'SELL' else 'BUY' end as order_type,
           cast(cast(flags as int) & 2 as boolean) as partial_fill,
           (case when (flags % 2) = 0
              then atoms_sold / limit_sell_amount
              else atoms_bought / limit_buy_amount
            end) as fill_proportion
    FROM trades_with_token_units trades
    JOIN uid_to_app_id
        ON uid = order_uid
    LEFT OUTER JOIN eth_flow_senders efs
        ON trades.order_uid = efs.order_uid
)

select *,
  -- Relative surplus (in %) is the difference between limit price and executed price as a ratio of the limit price.
  -- Absolute surplus (in USD) is relative surplus multiplied with the value of the trade
  usd_value * (atoms_bought * limit_sell_amount - atoms_sold * limit_buy_amount) / (atoms_bought * limit_sell_amount) as surplus_usd
from valued_trades
