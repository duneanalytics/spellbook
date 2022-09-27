{{ config(
        alias = 'pool_balance_changes',
        partition_by = ['day'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_address', 'nft_contract_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["niftytable"]\') }}'
        )
}}

{% set project_start_date = '2022-04-23' %}
{% set linear_bonding_address = '0x5b6ac51d9b1cede0068a1b26533cace807f883ee' %}

WITH
  pairs_created AS (
    SELECT
      CASE
        WHEN _bondingCurve = '{{linear_bonding_address}}' THEN 'linear_bonding'
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
      _fee AS initial_fee,
      output_pair AS pair_address,
      tx.FROM AS creator_address,
      date_trunc('day', call_block_time) AS day_created,
      cardinality(_initialNFTIDs) AS initial_nft_count,
      tx.value / 1e18 AS initial_eth
    FROM
      {{ source('sudo_amm_ethereum','LSSVMPairFactory_call_createPairETH') }} cre
      INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.hash = cre.call_tx_hash
        {% if not is_incremental() %}
        AND tx.block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
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
      date_trunc('day', et.evt_block_time) AS day,
      pair_address,
      SUM(CASE WHEN et.to = p.pair_address THEN 1 ELSE -1 END) AS nft_balance_change,
      0 AS eth_balance_change
    FROM
      {{ source('erc721_ethereum','evt_transfer') }} et
      INNER JOIN pairs_created p ON p.nft_contract_address = et.contract_address
      AND (et.to = p.pair_address OR et.from = p.pair_address)
    {% if not is_incremental() %}
    WHERE et.evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE et.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY
      1,2
  ),

  eth_balances AS (
    SELECT
      date_trunc('day',tr.block_time) AS day,
      pair_address,
      SUM(CASE WHEN tr.to = pc.pair_address THEN tr.value/1e18 ELSE -1*tr.value/1e18 END) AS eth_balance_change,
      0 AS nft_balance_change
    FROM
      {{ source('ethereum','traces') }} tr
      INNER JOIN pairs_created pc ON (pc.pair_address = tr.to OR pc.pair_address = tr.from)
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
      AND tr.block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
    GROUP BY
      1,2
  )

  SELECT
    bal.day AS day,
    bal.pair_address AS pool_address,
    COALESCE(bal.eth_balance_change, 0) AS eth_balance_change,
    COALESCE(bal.nft_balance_change, 0) AS nft_balance_change,
    pc.pricing_type AS pricing_type,
    pc.pool_type AS pool_type,
    pc.nft_contract_address AS nft_contract_address,
    pc.creator_address AS creator_address,
    pc.spot_price AS initial_price,
    pc.initial_nft_count AS initial_nft_count,
    pc.initial_eth AS initial_eth,
    pc.day_created AS day_created,
    COALESCE(mr.delta, pc.delta) AS delta, --if delta was never updated, just keep original deploy delta
    round(COALESCE(mr.spot_price, pc.spot_price), 4) as spot_price --same logic as above
  FROM
    (
      SELECT
        day,
        pair_address,
        COALESCE(SUM(eth_balance_change), 0) as eth_balance_change,
        COALESCE(SUM(nft_balance_change),0) as nft_balance_change
      FROM (
      SELECT * FROM erc721_balances
      UNION ALL
      SELECT * FROM eth_balances
      )
      GROUP BY 1,2
    ) bal
  INNER JOIN pairs_created pc ON pc.pair_address = bal.pair_address
  LEFT JOIN most_recent_spot_delta mr ON mr.pair_address = bal.pair_address
  ;