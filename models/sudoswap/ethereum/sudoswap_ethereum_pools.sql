{{
 config(
       alias='pools',
       partition_by = ['pool_address'],
       materialized = 'incremental',
       file_format = 'delta',
       incremental_strategy = 'merge',
       unique_key = ['pool_address', 'nft_contract_address', 'creator_address'],
       post_hook='{{ expose_spells(\'["ethereum"]\',
                                   "project",
                                   "sudoswap",
                                   \'["niftytable"]\') }}'
      )
}}

{% set project_start_date = '2022-04-23' %}

WITH
  pairs_created AS (
    SELECT
      CASE
        WHEN _bondingCurve = '0x5b6ac51d9b1cede0068a1b26533cace807f883ee' THEN 'linear_bonding'
        ELSE 'exp_bonding'
      END as pricing_type,
      _delta / 1e18 as delta,
      CASE
        WHEN _poolType = 0 THEN 'token'
        WHEN _poolType = 1 THEN 'nft'
        WHEN _poolType = 2 THEN 'trade'
      END AS pool_type,
      _spotPrice / 1e18 AS spot_price,
      _nft AS nft_contract_address,
      _initialNFTIDs AS nft_ids,
      _fee AS initialfee,
      output_pair AS pair_address,
      call_block_time AS block_time,
      tx.FROM AS creator_address,
      date_trunc('day', now()) - date_trunc('day', call_block_time) AS days_passed,
      cardinality(_initialNFTIDs) AS initial_nft_count,
      tx.value / 1e18 AS initial_eth
    FROM
      {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }} cre
      INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.hash = cre.call_tx_hash
    WHERE
      call_success
  ),
  most_recent_spot_delta AS (
    SELECT
      COALESCE(del.contract_address, spot.contract_address) AS pair_address,
      del.delta,
      spot.spot_price
    FROM
      (
        SELECT
          *
        FROM
          (
            SELECT
              contract_address,
              newDelta / 1e18 AS delta,
              row_number() over (
                PARTITION BY
                  contract_address
                ORDER BY
                  evt_block_number DESC,
                  evt_index DESC
              ) as most_recent
            FROM
              {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_DeltaUpdate') }}
          ) a
        WHERE
          most_recent = 1
      ) del
      FULL OUTER JOIN (
        SELECT
          *
        FROM
          (
            SELECT
              contract_address,
              newSpotPrice / 1e18 AS spot_price,
              row_number() over (
                PARTITION BY
                  contract_address
                ORDER BY
                  evt_block_number DESC,
                  evt_index DESC
              ) AS most_recent
            FROM
              {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_SpotPriceUpdate') }}
          ) a
        WHERE
          most_recent = 1
      ) spot ON del.contract_address = spot.contract_address
  ),
  erc721_balances AS (
    SELECT
      pair_address AS holder_address,
      SUM(
        CASE
          WHEN et.to = p.pair_address THEN 1
          ELSE -1
        END
      ) AS tokens_held
    FROM
      {{ source('erc721_ethereum','evt_transfer') }} et
      INNER JOIN pairs_created p ON p.nft_contract_address = et.contract_address
      AND (
        et.to = p.pair_address
        OR et.from = p.pair_address
      )
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run. We only want to update with new transfers.
    WHERE et.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY
      1
  ),
  eth_balances AS (
    WITH
      eth_in AS (
        SELECT
          tr.to AS holder_address,
          SUM(tr.value / 1e18) AS eth_funded
        FROM
          {{ source('ethereum','traces') }} tr
          JOIN pairs_created pc ON pc.pair_address = tr.to
        WHERE tr.success = true
          AND tr.type = 'call'
          AND (
            tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall')
            OR tr.call_type IS null
          )
          {% if not is_incremental() %}
          AND tr.block_time > '{{project_start_date}}'
          {% endif %}
          {% if is_incremental() %}
          -- this filter will only be applied on an incremental run. We only want to update with new traces.
          AND tr.block_time >= date_trunc("day", now() - interval '1 week')
          {% endif %}
        GROUP BY
          1
      ),
      eth_out AS (
        SELECT
          tr.FROM AS holder_address,
          SUM(tr.value / 1e18) AS eth_spent
        FROM
          {{ source('ethereum','traces') }} tr
          JOIN pairs_created pc ON pc.pair_address = tr.FROM
        WHERE tr.success = true
          AND tr.type = 'call'
          AND (
            tr.call_type NOT IN ('delegatecall', 'callcode', 'staticcall')
            OR tr.call_type IS null
          )
          {% if not is_incremental() %}
          AND tr.block_time > '{{project_start_date}}'
          {% endif %}
          {% if is_incremental() %}
          -- this filter will only be applied on an incremental run. We only want to update with new traces.
          AND tr.block_time >= date_trunc("day", now() - interval '1 week')
          {% endif %}
        GROUP BY
          1
      )
    SELECT
      eth_in.holder_address,
      eth_in.eth_funded,
      eth_out.eth_spent,
      COALESCE(eth_funded, 0) - COALESCE(eth_spent, 0) as eth_balance
    FROM
      eth_in
      LEFT JOIN eth_out ON eth_in.holder_address = eth_out.holder_address
  ),
  all_pairs_cleaned AS (
    SELECT
      pricing_type,
      pool_type,
      nft_contract_address,
      creator_address,
      COALESCE(mr.delta, pc.delta) AS delta --if delta was never updated, just keep original deploy delta
,
      round(COALESCE(mr.spot_price, pc.spot_price), 4) as spot_price --same logic as above
,
      nft_bal.tokens_held,
      round(COALESCE(eth_bal.eth_balance, 0), 4) as eth_balance,
      pc.pair_address AS raw_pair_address,
      days_passed,
      pc.spot_price AS initial_price,
      pc.initial_nft_count,
      pc.initial_eth
    FROM
      pairs_created pc
      LEFT JOIN most_recent_spot_delta mr ON pc.pair_address = mr.pair_address
      LEFT JOIN erc721_balances nft_bal ON nft_bal.holder_address = pc.pair_address
      LEFT JOIN eth_balances eth_bal ON eth_bal.holder_address = pc.pair_address
  ),
  trading_totals as (
    SELECT
      CASE
        WHEN trade_category = 'Sell' THEN buyer
        ELSE seller
      END AS pair_address,
      sum(amount_original) AS eth_volume,
      sum(amount_usd) AS usd_volume,
      sum(number_of_items) AS nfts_traded,
      sum(pool_fee_amount) AS owner_fee_volume_eth,
      sum(platform_fee_amount) AS platform_fee_volume_eth,
      sum(
        CASE
          WHEN trade_category = 'Sell' THEN -1 * amount_original
          ELSE (amount_original-platform_fee_amount)
        END
      ) AS eth_change_trading,
      sum(
        CASE
          WHEN trade_category = 'Sell' THEN number_of_items
          ELSE -1 * number_of_items
        END
      ) AS nft_change_trading
    FROM
      (
        SELECT
          trade_category,
          buyer,
          seller,
          amount_original,
          amount_usd,
          number_of_items,
          pool_fee_amount,
          platform_fee_amount
        FROM
          ({{ ref('sudoswap_ethereum_events') }})
        WHERE
          buyer IN (
            SELECT
              pair_address
            FROM
              pairs_created
          )
          OR seller IN (
            SELECT
              pair_address
            FROM
              pairs_created
          )
          {% if is_incremental() %}
          -- this filter will only be applied on an incremental run. We only want to update with new traces.
          AND block_time >= date_trunc("day", now() - interval '1 week')
          {% endif %}
      ) a
    GROUP BY
      1
  )
SELECT
  acc.raw_pair_address AS pool_address,
  nft_contract_address,
  creator_address,
  spot_price,
  COALESCE(tokens_held, 0) AS nft_balance,
  COALESCE(eth_balance, 0) AS eth_balance,
  COALESCE(trade.eth_volume, 0) AS eth_volume,
  COALESCE(trade.nfts_traded, 0) AS nfts_traded,
  COALESCE(trade.usd_volume, 0) AS usd_volume,
  COALESCE(trade.owner_fee_volume_eth, 0) AS owner_fee_volume_eth,
  COALESCE(trade.platform_fee_volume_eth, 0) as platform_fee_volume_eth,
  pool_type,
  pricing_type,
  delta,
  days_passed,
  initial_price,
  initial_nft_count,
  initial_eth,
  COALESCE(trade.eth_change_trading, 0) AS eth_change_trading,
  COALESCE(trade.nft_change_trading, 0) AS nft_change_trading
FROM
  all_pairs_cleaned acc
  LEFT JOIN trading_totals trade ON trade.pair_address = acc.raw_pair_address
