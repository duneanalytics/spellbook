{{
config(
      alias = 'pools',
      schema = 'sudoswap_ethereum',
      
      post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "sudoswap",
                                  \'["niftytable", "0xRob"]\') }}')
}}

WITH
  pool_balance AS (
    SELECT
      pool_address,
      SUM(eth_balance_change) AS eth_balance,
      SUM(nft_balance_change) AS nft_balance
    FROM
      {{ ref('sudoswap_ethereum_pool_balance_agg_day') }}
    GROUP BY pool_address
)

, pool_trade_stats AS (
    SELECT
      pool_address,
      SUM(eth_volume) AS eth_volume,
      SUM(nfts_traded) AS nfts_traded,
      SUM(usd_volume) AS usd_volume,
      SUM(pool_fee_volume_eth) AS pool_fee_volume_eth,
      SUM(pool_fee_bid_volume_eth) as pool_fee_bid_volume_eth,
      SUM(pool_fee_ask_volume_eth) as pool_fee_ask_volume_eth,
      SUM(platform_fee_volume_eth) as platform_fee_volume_eth,
      SUM(eth_change_trading) AS eth_change_trading,
      SUM(nft_change_trading) AS nft_change_trading
    FROM
      {{ ref('sudoswap_ethereum_pool_trades_agg_day') }}
    GROUP BY pool_address
)

SELECT
  p.pool_address,
  p.nft_contract_address,
  creator_address,
  eth_balance,
  nft_balance,
  coalesce(eth_volume,double '0') as eth_volume,
  coalesce(usd_volume,double '0') as usd_volume,
  coalesce(nfts_traded,uint256 '0') as nfts_traded,
  coalesce(pool_fee_volume_eth,double '0') as pool_fee_volume_eth,
  coalesce(pool_fee_bid_volume_eth,double '0') as pool_fee_bid_volume_eth,
  coalesce(pool_fee_ask_volume_eth,double '0') as pool_fee_ask_volume_eth,
  coalesce(platform_fee_volume_eth,double '0') as platform_fee_volume_eth,
  pool_type,
  p.bonding_curve,
  s.spot_price,
  s.delta,
  s.pool_fee,
  coalesce(eth_change_trading, double '0') as eth_change_trading,
  coalesce(nft_change_trading, int256 '0') as nft_change_trading,
  p.spot_price as initial_spot_price,
  initial_nft_balance,
  initial_eth_balance,
  pool_factory,
  creation_block_time,
  creation_tx_hash
FROM {{ ref('sudoswap_ethereum_pool_creations') }} p
INNER JOIN {{ ref('sudoswap_ethereum_pool_settings_latest') }} s
    ON p.pool_address = s.pool_address
INNER JOIN pool_balance b ON p.pool_address = b.pool_address
LEFT JOIN pool_trade_stats t ON p.pool_address = t.pool_address
