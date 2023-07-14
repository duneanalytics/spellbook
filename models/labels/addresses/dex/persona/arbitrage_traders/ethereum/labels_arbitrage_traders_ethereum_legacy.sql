{{config(
	tags=['legacy'],
	alias = alias('arbitrage_traders_ethereum', legacy_model=True))}}

with 
 eth_arb_traders as (
    with
      pools as (
        -- uni v2 pools
        select pair
        from uniswap_v2_ethereum.Factory_evt_PairCreated
        union
        
        -- uni v3 pools
        select pool as pair
        from uniswap_v3_ethereum.Factory_evt_PoolCreated
      ),
      err_contracts as (
        select address
        from
          (
            VALUES
              --
              ('0x7a250d5630b4cf539739df2c5dacb4c659f2488d'), -- uniswap router02
              ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45'), -- uniswap swaprouter02
              ('0xe592427a0aece92de3edee1f18e0157c05861564'), -- uniswap v3 router
              ('0xd47140f6ab73f6d6b6675fb1610bb5e9b5d96fe5'), -- 1inch
              ('0xe66b31678d6c16e9ebf358268a790b763c133750'), -- 0x
              ('0x1111111254fb6c44bac0bed2854e76f90643097d'), -- 1inch
              ('0x11111112542d85b3ef69ae05771c2dccff4faa26'), -- 1inch
              ('0xe069cb01d06ba617bcdf789bf2ff0d5e5ca20c71'), -- 1inch
              ('0xdef1c0ded9bec7f1a1670819833240f027b25eff'), -- 0x
              ('0xf2f400c138f9fb900576263af0bc7fcde2b1b8a8'), -- 1inch
              ('0x220bda5c8994804ac96ebe4df184d25e5c2196d4'), -- 1inch
              ('0x619b188b3f02605e289771e0001f9c313b8436a0'), -- aggregator
              ('0xdb38ae75c5f44276803345f7f02e95a0aeef5944'), -- 1inch
              ('0x775ee938186fddc13bd7c89d24820e1b0758f91d'), -- zapper.fi
              ('0x31e085afd48a1d6e51cc193153d625e8f0514c7f'), -- kyber
              ('0x22f9dcf4647084d6c31b2765f6910cd85c178c18'), -- 0x
              ('0x9008d19f58aabd9ed0d60971565aa8510560ab41'), -- cow
              ('0xfade503916c1d1253646c36c9961aa47bf14bd2d'), -- 1inch
              ('0x9021c84f3900b610ab8625d26d739e3b7bff86ab'), -- 1inch
              ('0x11111254369792b2ca5d084ab5eea397ca8fa48b'), -- 1inch
              ('0xf7ca8f55c54cbb6d0965bc6d65c43adc500bc591'), -- unknown protocol
              ('0xdef171fe48cf0115b1d80b88dc8eab59176fee57'), -- paraswap
              ('0x54a4a1167b004b004520c605e3f01906f683413d'), -- kyber
              ('0x288931fa76d7b0482f0fd0bca9a50bf0d22b9fef'), -- 1inch
              ('0x8df6084e3b84a65ab9dd2325b5422e5debd8944a') -- coinbase wallet swap proxy
          ) as x (address)
      )
    SELECT 
      distinct t1.taker as address
    FROM
      (
        SELECT taker,
               tx_hash,
               blockchain,
               token_sold_address,
               token_bought_address,
               evt_index
        FROM {{ref('dex_trades_legacy')}}

        UNION ALL

        SELECT taker,
               tx_hash,
               blockchain,
               token_sold_address,
               token_bought_address,
               evt_index
        FROM {{ref('dex_aggregator_trades_legacy')}}
      ) t1
      INNER JOIN
      (
        SELECT taker,
               tx_hash,
               blockchain,
               token_sold_address,
               token_bought_address,
               evt_index
        FROM {{ref('dex_trades_legacy')}}
        UNION ALL
        SELECT taker,
               tx_hash,
               blockchain,
               token_sold_address,
               token_bought_address,
               evt_index
        FROM {{ref('dex_aggregator_trades_legacy')}}
      ) t2 ON t1.tx_hash = t2.tx_hash
    WHERE
      t1.blockchain = 'ethereum'
      AND t2.blockchain = 'ethereum'
      AND t1.token_sold_address = t2.token_bought_address
      AND t1.token_bought_address = t2.token_sold_address
      AND t1.evt_index != t2.evt_index
      AND t1.taker not in (
        select pair from pools
      )
      AND t1.taker not in (
        select address from err_contracts
      )
      AND t1.taker = t2.taker
  )
select
  "ethereum" as blockchain,
  address,
  "Arbitrage Trader" AS name,
  "dex" AS category,
  "alexth" AS contributor,
  "query" AS source,
  timestamp('2022-10-05') as created_at,
  now() as updated_at,
  "arbitrage_traders" as model_name,
  "persona" as label_type
from
  eth_arb_traders