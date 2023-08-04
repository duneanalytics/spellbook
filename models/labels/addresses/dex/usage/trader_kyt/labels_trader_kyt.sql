{{
    config(
        alias = alias('kyt'),
        post_hook='{{ expose_spells(\'["ethereum", "fantom", "arbitrum", "avalanche_c", "gnosis", "bnb", "optimism", "polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["whiskey"]\') }}'
    )
}}

with initial_bot_list AS (
select distinct `from`
from (select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('ethereum', 'transactions') }} t1
      INNER JOIN (select distinct `from`
        from {{ source('ethereum', 'transactions') }}
        where block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
      union all
      select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('polygon', 'transactions') }} t1
        INNER JOIN (select distinct `from`
        from {{ source('polygon', 'transactions') }}
        where
        block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
      union all
      select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('optimism', 'transactions') }} t1
        INNER JOIN (select distinct `from`
        from {{ source('optimism', 'transactions') }}
        where block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
      union all
      select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('arbitrum', 'transactions') }} t1
        INNER JOIN (select distinct `from`
        from {{ source('arbitrum', 'transactions') }}
        where block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
      union all
      select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('gnosis', 'transactions') }} t1
        INNER JOIN (select distinct `from`
        from {{ source('gnosis', 'transactions') }}
        where block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
      union all
      select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('fantom', 'transactions') }} t1
        INNER JOIN (select distinct `from`
        from {{ source('fantom', 'transactions') }}
        where block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
      union all
      select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('bnb', 'transactions') }} t1
        INNER JOIN (select distinct `from`
        from {{ source('bnb', 'transactions') }}
        where block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
      union all
      select t1.`from`, date_trunc('month', t1.block_time) AS month, count(*) AS num_tx
      from {{ source('avalanche_c', 'transactions') }} t1
        INNER JOIN (select distinct `from`
        from {{ source('avalanche_c', 'transactions') }}
        where block_time > now() - interval '30' day) t2 on t1.`from` = t2.`from`
      group by 1, 2
      having count(*) > 1000
))
 
, final_bot_list AS (
    select address, 'Likely Bot' AS trader_type
    from (
        select cast(`from` AS varchar(100)) AS address
        from initial_bot_list t1
        except (
            select address
            from {{ ref('labels_cex') }}
            union all 
            select address
            from {{ ref('labels_miners') }}
            union all
            select
                cast (lower(address) AS varchar (100))
            from
           (VALUES
           ('0x7ed1e469fcb3ee19c0366d829e291451be638e59', 'https://freewallet.org/')
               , ('0x2E05A304d3040f1399c8C20D2a9F659AE7521058', 'https://www.luno.com/')
               , ('0xe3031C1BfaA7825813c562CbDCC69d96FCad2087', 'Gopax: WBTC Merchant Deposit Address')
               , ('0x3c02290922a3618a4646e3bbca65853ea45fe7c6', 'https://indodax.com/ exchange')
               , ('0xFDeda15e2922C5ed41fc1fdF36DA2FB2623666b3', 'HitBTC: Deposit Funder ')
               , ('0xffec0067f5a79cff07527f63d83dd5462ccf8ba4', 'Nexo')
               , ('0x912fd21d7a69678227fe6d08c64222db41477ba0', 'shakepay')
               , ('0x00472c1e4275230354dbe5007A5976053f12610a', 'https://www.acron, ‘.com/en-us/')
               , ('0x734ac651dd95a339c633cded410228515f97faff', 'ZB.com: Deposit Funder')
               , ('0xc00eebe4e2be29679781fc5fc350057ee8132bab', 'LAToken Wallet')
               , ('0xae45a8240147e6179ec7c9f92c5a18f9a97b3fca', 'Crypto.com: Deposit Funder')
               , ('0xA4e5961B58DBE487639929643dCB1Dc3848dAF5E', 'changenow: gas supplier')
               , ('0xcDd37Ada79F589c15bD4f8fD2083dc88E34A2af2', 'SideShift: Hot Wallet')
               , ('0xC88F7666330b4b511358b7742dC2a3234710e7B1', 'blockchain.com')
               , ('0x72E5263FF33D2494692D7F94A758aA9F82062F73', 'probit cex')
               , ('0x03599A2429871E6be1B154Fb9c24691F9D301865', 'bithump gas supplier')
               , ('0x0031e147A79c45f24319dc02ca860cB6142FCBA1', 'nexo deployer')
               , ('0x4FED1fC4144c223aE3C1553be203cDFcbD38C581', 'crptonator Wallet')
               , ('0xf646cbe3b030fb6c2569215f0117dba58badb95e', 'Bitbay')
               , ('0x32B56FC48684FA085dF8C4cd2feAAfC25c304dB9', 'related to korbit exchange')
               , ('0x55fe002aeff02f77364de339a1292923a15844b8', 'Circle')
               , ('0xed212a4a2e82d5ee0d62f70b5dee2f5ee0f10c5d', 'Nexo deployer')
               , ('0x58f56615180a8eea4c462235d9e215f72484b4a3', 'Deribit hot Wallet')
               , ('0x7aeb3314e041153c4f6bbea19abecbce20946fd4', 'related to WazirX ')
               , ('0xc94ebb328ac25b95db0e0aa968371885fa516215', 'Roobet')
               , ('0x777f415324d56e1d54fa832902d8797db7a4c57c', '1xbet ')
               , ('0xF27696C8BCa7D54D696189085Ae1283f59342fA6', 'Argent relayer')
               , ('0x09344477fdc71748216a7b8bbe7f2013b893def8', 'ascendex deposit address')
               , ('0xe4b3dd9839ed1780351dc5412925cf05f07a1939', 'Coinspot exchange')
               , ('0x9aa65464b4cfbe3dc2bdb3df412aee2b3de86687', 'blokchain.com')
               , ('0xa305fab8bda7e1638235b054889b3217441dd645', 'changenow binance deposit address')
               , ('0xc5a8859c44ac8aa2169afacf45b87c08593bec10', 'Paxos')
               , ('0x95b564f3b3bae3f206aa418667ba000afafacc8a', 'Bitvavo')
               , ('0xd2c82f2e5fa236e114a81173e375a73664610998', 'coinl')
               , ('0xc9f5296eb3ac266c94568d790b6e91eba7d76a11', 'Cex.io ')
               , ('0x808b4da0be6c9512e948521452227efc619bea52', 'BlockFi')
               , ('0x6dfc34609a05bc22319fa4cce1d1e2929548c0d7', 'Bovada Gambeling')
               , ('0xc5a93444cc4da6efb9e6fc6e5d3cb55a53b52396', 'Okex deposit address')
               , ('0x260ee8f2b0c167e0cd6119b2df923fd061dc1093', 'Youhodler')
               , ('0x05cdb1526f6e224e02919a4c018d9784ea25eb3d', 'Luno')
               , ('0xb275483694d627242e77b95630168da4aec5da83', 'OSL')
               , ('0xeea5e6ea5998928352cf54425819436f80f95ae9', 'EXMO deployer')
               , ('0x618ffd1cdabee36ce5992a857cc7463f21272bd7', 'Wazirx withdrawal')
               , ('0x1f973b233f5ebb1e5d7cfe51b9ae4a32415a3a08', 'Cex.io ')
               , ('0x6a3eb79e1c4023f1610ff046c5dc30f9790d326f', 'Coinbene hot wallet')
               , ('0x832F166799A407275500430b61b622F0058f15d6', 'BTC Turk ')
               , ('0x1938a448d105d26c40a52a1bfe99b8ca7a745ad0', 'Upbit')
               , ('0xd30b438df65f4f788563b2b3611bd6059bff4ad9', 'OKEX')
               , ('0x8365efb25d0822aaf15ee1d314147b6a7831c403', 'Remitano: Deposit Funder')
               , ('0x1a1c87d9a6f55d3bbb064bff1059ad37b6bdc097', 'Bitvavo gas supplier')
               , ('0x27e9f4748a2eb776be193a1f7dec2bb6daafe9cf', 'Houbi')
               , ('0x0084dfd7202e5f5c0c8be83503a492837ca3e95e', 'Bithump deposit')
               , ('0x6c3F07bfDfce0463457385129fed97F0992a111d', 'Poloniex exchange deployer')
               , ('0x986ccf5234d9cfbb25246f1a5bfa51f4ccfcb308', 'Binance deposit')
               , ('0xC333E80eF2deC2805F239E3f1e810612D294F771', 'Alameda Research')
               , ('0x20312e96b1a0568ac31c6630844a962383cc66c2', 'Coinspot gas supplir')
               , ('0x73f9b272abda7a97cb1b237d85f9a7236edb6f16', 'Binance Deposit')
           ) AS data (address, label)
       )
    )
)

,active_traders AS (
    SELECT tx_from,
           sum(amount_usd)                                                            AS trade_amount,
           case
               when sum(amount_usd) >= cast(10000 AS double) and
                    sum(amount_usd) < cast(100000 AS double) then 'Turtle trader'
               when sum(amount_usd) >= cast(100000 AS double) and
                    sum(amount_usd) < cast(500000 AS double) then 'Shark trader'
               when sum(amount_usd) >= cast(500000 AS double) then 'Whale trader' end AS trader_type
    from {{ ref('dex_trades') }}
    where block_time > now() - interval '30' day
    group by 1
    having sum(amount_usd) > cast (10000 AS double)
)

, Former_traders AS (
    SELECT t.tx_from,
           case
               when t.monthly_trade_amount >= cast(10000 AS double) and
                    t.monthly_trade_amount < cast(100000 AS double) then 'Former Turtle trader'
               when t.monthly_trade_amount >= cast(100000 AS double) and
                    t.monthly_trade_amount < cast(500000 AS double) then 'Former Shark trader'
               when t.monthly_trade_amount >= cast(500000 AS double)
                   then 'Former Whale trader' end AS trader_type
    FROM (
        SELECT t1.tx_from,
               date_trunc('month', t1.block_time)                                           AS month,
               sum(t1.amount_usd)                                                           AS monthly_trade_amount,
               ROW_NUMBER() OVER (PARTITION BY t1.tx_from ORDER BY sum(t1.amount_usd) DESC) AS rn
        FROM {{ ref('dex_trades') }} t1
            join (select distinct tx_from from {{ ref('dex_trades') }} where block_time > now() - interval '3' month ) t3
        on t3.tx_from = t1.tx_from
            left join active_traders t2 on t1.tx_from = t2.tx_from
        where t1.block_time >= now() - interval '1' year
          and t1.block_time
            < now() - interval '30' day
          and t2.tx_from is NULL
        GROUP BY 1, 2
        HAVING sum(t1.amount_usd) > cast (10000 AS double)
        ) t
    WHERE t.rn = 1
    ORDER BY t.month, t.monthly_trade_amount DESC
)

, final AS (
    SELECT cast(tx_from AS varchar(100)) AS address,
           trader_type
    from active_traders
    union all
    SELECT cast(tx_from AS varchar(100)) AS address,
           trader_type
    from Former_traders
    union all
    SELECT address,
           trader_type
    from final_bot_list
)

select "multi"                         AS blockchain
     , address
     , trader_type                     AS name
     , "dex"                           AS category
     , "whiskey"                       AS contributor
     , "query"                         AS source
     , cast('2023-03-05' AS timestamp) AS created_at
     , now()                           AS updated_at
     , "usage"                         AS label_type
     , "KYT.DexGuru"                   AS model_name
from final
;
