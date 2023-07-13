{{  config(
        alias=alias('trades', legacy_model=True),
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['tx_hash', 'order_uid', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha"]\') }}'
    )
}}

-- Find the PoC Query here: https://dune.com/queries/1719733
WITH
-- First subquery joins buy and sell token prices from prices.usd
-- Also deducts fee from sell amount
trades_with_prices AS (
    SELECT try_cast(date_trunc('day', evt_block_time) as date) as block_date,
           evt_block_time            as block_time,
           evt_tx_hash               as tx_hash,
           evt_index,
           settlement.contract_address          as project_contract_address,
           owner                     as trader,
           orderUid                  as order_uid,
           sellToken                 as sell_token,
           buyToken                  as buy_token,
           (sellAmount - feeAmount)  as sell_amount,
           buyAmount                 as buy_amount,
           feeAmount                 as fee_amount,
           ps.price                  as sell_price,
           pb.price                  as buy_price
    FROM {{ source('gnosis_protocol_v2_gnosis', 'GPv2Settlement_evt_Trade') }} settlement
             LEFT OUTER JOIN {{ source('prices', 'usd') }} as ps
                             ON sellToken = ps.contract_address
                                 AND ps.minute = date_trunc('minute', evt_block_time)
                                 AND ps.blockchain = 'gnosis'
                                 {% if is_incremental() %}
                                 AND ps.minute >= date_trunc("day", now() - interval '1 week')
                                 {% endif %}
             LEFT OUTER JOIN {{ source('prices', 'usd') }} as pb
                             ON pb.contract_address = (
                                 CASE
                                     WHEN buyToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d'
                                     ELSE buyToken
                                     END)
                                 AND pb.minute = date_trunc('minute', evt_block_time)
                                 AND pb.blockchain = 'gnosis'
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
                WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'xDAI'
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
             LEFT OUTER JOIN {{ ref('tokens_gnosis_erc20_legacy') }} ts
                             ON ts.contract_address = sell_token
             LEFT OUTER JOIN {{ ref('tokens_gnosis_erc20_legacy') }} tb
                             ON tb.contract_address =
                                (CASE
                                     WHEN buy_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                                         THEN '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d'
                                     ELSE buy_token
                                    END)
),
-- This, independent, aggregation defines a mapping of order_uid and trade
-- TODO - create a view for the following block mapping uid to app_data
order_ids as (
    select evt_tx_hash, collect_list(orderUid) as order_ids
    from (  select orderUid, evt_tx_hash, evt_index
            from {{ source('gnosis_protocol_v2_gnosis', 'GPv2Settlement_evt_Trade') }}
             {% if is_incremental() %}
             where evt_block_time >= date_trunc("day", now() - interval '1 week')
             {% endif %}
                     sort by evt_index
         ) as _
    group by evt_tx_hash
),

exploded_order_ids as (
    select evt_tx_hash, posexplode(order_ids)
    from order_ids
),

reduced_order_ids as (
    select
        col as order_id,
        -- This is a dirty hack!
        collect_list(evt_tx_hash)[0] as evt_tx_hash,
        collect_list(pos)[0] as pos
    from exploded_order_ids
    group by order_id
),

trade_data as (
    select call_tx_hash,
           posexplode(trades)
    from {{ source('gnosis_protocol_v2_gnosis', 'GPv2Settlement_call_settle') }}
    where call_success = true
    {% if is_incremental() %}
    AND call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

uid_to_app_id as (
    select
        order_id as uid,
        get_json_object(trades.col, '$.appData') as app_data,
        get_json_object(trades.col, '$.receiver') as receiver,
        get_json_object(trades.col, '$.sellAmount') as limit_sell_amount,
        get_json_object(trades.col, '$.buyAmount') as limit_buy_amount,
        get_json_object(trades.col, '$.validTo') as valid_to,
        get_json_object(trades.col, '$.flags') as flags
    from reduced_order_ids order_ids
             join trade_data trades
                  on evt_tx_hash = call_tx_hash
                      and order_ids.pos = trades.pos
),

valued_trades as (
    SELECT block_date,
           block_time,
           tx_hash,
           evt_index,
           CAST(ARRAY() as array<bigint>) AS trace_address,
           project_contract_address,
           order_uid,
           trader,
           sell_token_address,
           sell_token,
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
             -- Note that this formulation is subject to some precision error in a few irregular cases:
             -- E.g. In this transaction 0x84d57d1d57e01dd34091c763765ddda6ff713ad67840f39735f0bf0cced11f02
             -- buy_price * units_bought * fee / units_sold
             -- 1.001076 * 0.005 * 0.0010148996324193 / 3e-18 = 1693319440706.3
             -- So, if sell_price had been null here (thankfully it is not), we would have a vastly inaccurate fee valuation
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
           flags
    FROM trades_with_token_units
             JOIN uid_to_app_id
                  ON uid = order_uid
)

select *,
  -- Relative surplus (in %) is the difference between limit price and executed price as a ratio of the limit price.
  -- Absolute surplus (in USD) is relative surplus multiplied with the value of the trade
  usd_value * (((limit_sell_amount / limit_buy_amount) - (atoms_sold/atoms_bought)) / (limit_sell_amount / limit_buy_amount)) as surplus_usd
from valued_trades
