{{
config(
      alias='pools',
      post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "project",
                                  "sudoswap",
                                  \'["niftytable"]\') }}')
}}

WITH pool_stats AS (
SELECT
  pool_address,
  nft_contract_address,
  creator_address,
  pool_type,
  pricing_type,
  initial_price,
  initial_nft_count,
  initial_eth,
  day_created,
  delta,
  spot_price
FROM
  {{ ref('sudoswap_ethereum_pool_balance_changes') }}
ORDER BY DAY DESC
LIMIT 1 -- To get the most recent delta and spot price
),

pool_balance AS (
SELECT
  pool_address,
  SUM(nft_balance_change) AS nft_balance,
  SUM(eth_balance_change) AS eth_balance
FROM
  {{ ref('sudoswap_ethereum_pool_balance_changes') }}
GROUP BY 1
),

pool_trades AS (
SELECT
  pool_address,
  SUM(eth_volume) AS eth_volume,
  SUM(nfts_traded) AS nfts_traded,
  SUM(usd_volume) AS usd_volume,
  SUM(owner_fee_volume_eth) AS owner_fee_volume_eth,
  SUM(platform_fee_volume_eth) as platform_fee_volume_eth,
  SUM(eth_change_trading) AS eth_change_trading,
  SUM(nft_change_trading) AS nft_change_trading
FROM
  {{ ref('sudoswap_ethereum_pool_trades') }}
GROUP BY 1
)

SELECT
  ps.pool_address AS pool_address,
  nft_contract_address,
  creator_address,
  spot_price,
  pool_type,
  pricing_type,
  delta,
  day_created,
  initial_price,
  initial_nft_count,
  initial_eth,
  nft_balance,
  eth_balance,
  eth_volume,
  nfts_traded,
  usd_volume,
  owner_fee_volume_eth,
  platform_fee_volume_eth,
  eth_change_trading,
  nft_change_trading
FROM
  pool_stats ps
INNER JOIN pool_balance pb ON pb.pool_address = ps.pool_address
INNER JOIN pool_trades pt ON pt.pool_address = ps.pool_address
;