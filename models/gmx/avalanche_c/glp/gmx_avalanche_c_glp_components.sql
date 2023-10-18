{{ config(
        alias = alias('glp_components'),
        materialized = 'incremental',
        partition_by = ['block_date'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'minute'],
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "gmx",
                                    \'["theachenyj"]\') }}'
        )
}}
/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/
{% set project_start_date = '2021-12-22 06:07' %}

with minute as -- This CTE generates a series of minute values
         (
            {% if not is_incremental() %}
            SELECT explode(sequence(TIMESTAMP '{{project_start_date}}', CURRENT_TIMESTAMP, INTERVAL 1 minute)) AS minute
            {% endif %}
            {% if is_incremental() %}
            SELECT explode(sequence(date_trunc("day", now() - interval '1 week'), CURRENT_TIMESTAMP, INTERVAL 1 minute)) AS minute
            {% endif %}
         ),
     token as -- This CTE create tokens which in GLP pool on Avalanche
         (
             select token,
                    symbol,
                    decimals
             from (
                      values
                          ('0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e', 'USDC', 6.0),
                          ('0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab', 'WETH.e', 18.0),
                          ('0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664', 'USDC.e', 6.0),
                          ('0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'WAVAX', 18.0),
                          ('0x50b7545627a5162f82a992c33b87adc75187b218', 'WBTC.e', 8.0),
                          ('0x130966628846bfd36ff31a822705796e8cb8c18d', 'MIM', 18.0),
                          ('0x152b9d0fdc40c096757f570a51e494bd4b943e50', 'BTC.b', 8.0) )
                      as t(token, symbol, decimals)
         ),
     minute_token as -- This CTE combine tokens and a series of minute values
         (
             select *
             from minute,
                  token
         ),
     pool_amount as -- This CTE returns the average amount of each token in the pool for a designated minute
         (
             with pool_amount_raw as (
                 SELECT date_trunc('minute', call_block_time) AS block_minute,
                        output_0                              as amount,
                        _0                                    as token
                 FROM {{source('gmx_avalanche_c', 'Vault_call_poolAmounts')}}
                 where call_success = true
                 {% if is_incremental() %}
                 AND call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                 {% if not is_incremental() %}
                 AND call_block_time >= '{{project_start_date}}'
                 {% endif %}
             )
             select block_minute,
                    token,
                    avg(amount) as amount
             from pool_amount_raw
             group by 1, 2
         ),
     rsv_amount as -- This CTE returns the average amount of each reserved token in the pool for a designated minute
         (
             with rsv_amount_raw as (
                 select date_trunc('minute', call_block_time) as block_minute,
                        output_0                              as amount,
                        _0                                    as token
                 FROM {{source('gmx_avalanche_c', 'Vault_call_reservedAmounts')}}
                 where call_success = true
                 {% if is_incremental() %}
                 AND call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                 {% if not is_incremental() %}
                 AND call_block_time >= '{{project_start_date}}'
                 {% endif %}
             )
             select block_minute,
                    token,
                    avg(amount) as amount
             from rsv_amount_raw
             group by 1, 2
         ),
     grt_usd as -- This CTE returns the guaranteed USD amount of each token in the pool for a designated minute
         (
             with grt_usd_raw as (
                 select date_trunc('minute', call_block_time) as block_minute,
                        output_0                              as amount,
                        _0                                    as token
                 FROM {{source('gmx_avalanche_c', 'Vault_call_guaranteedUsd')}}
                 where call_success = true
                 {% if is_incremental() %}
                 AND call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                 {% if not is_incremental() %}
                 AND call_block_time >= '{{project_start_date}}'
                 {% endif %}
             )
             select block_minute,
                    token,
                    avg(amount) as amount
             from grt_usd_raw
             group by 1, 2
         ),
     max_price as -- This CTE returns the maximum price of each token in the pool for a designated minute
         (
             with max_price_raw as (
                 select date_trunc('minute', call_block_time) as block_minute,
                        output_0                              as price,
                        _token                                as token
                 FROM {{source('gmx_avalanche_c', 'Vault_call_getMaxPrice')}}
                 where call_success = true
                 {% if is_incremental() %}
                 AND call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                 {% if not is_incremental() %}
                 AND call_block_time >= '{{project_start_date}}'
                 {% endif %}
             )
             select block_minute,
                    token,
                    avg(price) as price
             from max_price_raw
             group by 1, 2
         ),
     min_price as -- This CTE returns the minimum price of each token in the pool for a designated minute
         (
             with min_price_raw as (
                 select date_trunc('minute', call_block_time) as block_minute,
                        output_0                              as price,
                        _token                                as token
                 FROM {{source('gmx_avalanche_c', 'Vault_call_getMinPrice')}}
                 where call_success = true
                 {% if is_incremental() %}
                 AND call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                 {% if not is_incremental() %}
                 AND call_block_time >= '{{project_start_date}}'
                 {% endif %}
             )
             select block_minute,
                    token,
                    avg(price) as price
             from min_price_raw
             group by 1, 2
         ),
     global_short_average_price
         as -- This CTE returns volume weighted average price of each token short for a designated minute
         (
             with global_short_average_price_raw as (
                 select date_trunc('minute', call_block_time) as block_minute,
                        output_0                              as price,
                        _0                                    as token
                 FROM {{source('gmx_avalanche_c', 'Vault_call_globalShortAveragePrices')}}
                 where call_success = true
                 {% if is_incremental() %}
                 AND call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                 {% if not is_incremental() %}
                 AND call_block_time >= '{{project_start_date}}'
                 {% endif %}
             )
             select block_minute,
                    token,
                    avg(price) as price
             from global_short_average_price_raw
             group by 1, 2
         ),
     global_short_size as -- This CTE returns average sum of each tokens short for a designated minute
         (
             with global_short_size_raw as (
                 select date_trunc('minute', call_block_time) as block_minute,
                        output_0                              as amount,
                        _0                                    as token
                 FROM {{source('gmx_avalanche_c', 'Vault_call_globalShortSizes')}}
                 where call_success = true
                 {% if is_incremental() %}
                 AND call_block_time >= date_trunc("day", now() - interval '1 week')
                 {% endif %}
                 {% if not is_incremental() %}
                 AND call_block_time >= '{{project_start_date}}'
                 {% endif %}
             )
             select block_minute,
                    token,
                    avg(amount) as amount
             from global_short_size_raw
             group by 1, 2
         ),
     final as ( -- This CTE is meat to do some cleaning action
         select minute,
                token,
                symbol,
                decimals,
                coalesce(pool_amount / power(10, decimals), 0)     as pool_amount,
                coalesce(reserved_amount / power(10, decimals), 0) as reserved_amount,
                coalesce(guaranteed_usd / 1e30, 0)                 as guaranteed_usd,
                coalesce(max_price / 1e30, 0)                      as max_price,
                coalesce(min_price / 1e30, 0)                      as min_price,
                coalesce(global_short_average_price / 1e30, 0)     as global_short_average_price,
                coalesce(global_short_size / 1e30, 0)              as global_short_size
         from (
                  select m.minute,
                         m.token,
                         m.symbol,
                         m.decimals,
                         last(pm.amount, true) OVER (partition by m.token order by m.minute)  as pool_amount,
                         last(rm.amount, true) OVER (partition by m.token order by m.minute)  as reserved_amount,
                         last(gu.amount, true) OVER (partition by m.token order by m.minute)  as guaranteed_usd,
                         last(mxp.price, true) OVER (partition by m.token order by m.minute)  as max_price,
                         last(mnp.price, true) OVER (partition by m.token order by m.minute)  as min_price,
                         last(gsap.price, true) OVER (partition by m.token order by m.minute) as global_short_average_price,
                         last(gss.amount, true) OVER (partition by m.token order by m.minute) as global_short_size
                  from minute_token as m
                           left join pool_amount as pm on m.minute = pm.block_minute and m.token = pm.token
                           left join rsv_amount as rm on m.minute = rm.block_minute and m.token = rm.token
                           left join grt_usd as gu on m.minute = gu.block_minute and m.token = gu.token
                           left join max_price as mxp on m.minute = mxp.block_minute and m.token = mxp.token
                           left join min_price as mnp on m.minute = mnp.block_minute and m.token = mnp.token
                           left join global_short_average_price as gsap
                                     on m.minute = gsap.block_minute and m.token = gsap.token
                           left join global_short_size as gss on m.minute = gss.block_minute and m.token = gss.token
              ) as tmp_win
     )
-- povit and get final output
select usdc.minute                                      as minute,
       TRY_CAST(date_trunc('DAY', usdc.minute) AS date) as block_date,
       usdc.available_assets                            as usdc_available_assets,
       usdc.current_price                               as usdc_current_price,
       usdc_e.available_assets                          as usdc_e_available_assets,
       usdc_e.current_price                             as usdc_e_current_price,
       mim.available_assets                             as mim_available_assets,
       mim.current_price                                as mim_current_price,
       wavax.available_assets                           as wavax_available_assets,
       wavax.current_price                              as wavax_current_price,
       wavax.longs                                      as wavax_longs,
       wavax.shorts_entry_price                         as wavax_shorts_entry_price,
       wavax.shorts_outstanding_notional                as wavax_shorts_outstanding_notional,
       weth_e.available_assets                          as weth_e_available_assets,
       weth_e.current_price                             as weth_e_current_price,
       weth_e.longs                                     as weth_e_longs,
       weth_e.shorts_entry_price                        as weth_e_shorts_entry_price,
       weth_e.shorts_outstanding_notional               as weth_e_shorts_outstanding_notional,
       wbtc_e.available_assets                          as wbtc_e_available_assets,
       wbtc_e.current_price                             as wbtc_e_current_price,
       wbtc_e.longs                                     as wbtc_e_longs,
       wbtc_e.shorts_entry_price                        as wbtc_e_shorts_entry_price,
       wbtc_e.shorts_outstanding_notional               as wbtc_e_shorts_outstanding_notional,
       btc_b.available_assets                           as btc_b_available_assets,
       btc_b.current_price                              as btc_b_current_price,
       btc_b.longs                                      as btc_b_longs,
       btc_b.shorts_entry_price                         as btc_b_shorts_entry_price,
       btc_b.shorts_outstanding_notional                as btc_b_shorts_outstanding_notional
from (
         select minute,
                pool_amount                                                as available_assets,
                ((max_price + min_price) + abs(max_price - min_price)) / 2 as current_price
         from final
         where symbol = 'USDC'
     ) as usdc
         left join
     (
         select minute,
                pool_amount                                                as available_assets,
                ((max_price + min_price) + abs(max_price - min_price)) / 2 as current_price
         from final
         where symbol = 'USDC.e'
     ) as usdc_e
     on usdc.minute = usdc_e.minute
         left join
     (
         select minute,
                pool_amount                                                as available_assets,
                ((max_price + min_price) + abs(max_price - min_price)) / 2 as current_price
         from final
         where symbol = 'MIM'
     ) as mim on usdc.minute = mim.minute
         left join
     (
         select minute,
                pool_amount - reserved_amount                              as available_assets,
                ((max_price + min_price) + abs(max_price - min_price)) / 2 as current_price,
                guaranteed_usd                                             as longs,
                global_short_average_price                                 as shorts_entry_price,
                global_short_size                                          as shorts_outstanding_notional
         from final
         where symbol = 'WAVAX'
     ) as wavax on usdc.minute = wavax.minute
         left join
     (
         select minute,
                pool_amount - reserved_amount                              as available_assets,
                ((max_price + min_price) + abs(max_price - min_price)) / 2 as current_price,
                guaranteed_usd                                             as longs,
                global_short_average_price                                 as shorts_entry_price,
                global_short_size                                          as shorts_outstanding_notional
         from final
         where symbol = 'WETH.e'
     ) as weth_e on usdc.minute = weth_e.minute
         left join
     (
         select minute,
                pool_amount - reserved_amount                              as available_assets,
                ((max_price + min_price) + abs(max_price - min_price)) / 2 as current_price,
                guaranteed_usd                                             as longs,
                global_short_average_price                                 as shorts_entry_price,
                global_short_size                                          as shorts_outstanding_notional
         from final
         where symbol = 'WBTC.e'
     ) as wbtc_e on usdc.minute = wbtc_e.minute
         left join
     (
         select minute,
                pool_amount - reserved_amount                              as available_assets,
                ((max_price + min_price) + abs(max_price - min_price)) / 2 as current_price,
                guaranteed_usd                                             as longs,
                global_short_average_price                                 as shorts_entry_price,
                global_short_size                                          as shorts_outstanding_notional
         from final
         where symbol = 'BTC.b'
     ) as btc_b on usdc.minute = btc_b.minute
