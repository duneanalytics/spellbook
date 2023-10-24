{{ config(
    
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['version', 'tokenid', 'token', 'pool'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "curvefi",
                                \'["msilb7"]\') }}'
    )
}}

-- Original Ref - Dune v1 Abstraction: https://github.com/duneanalytics/spellbook/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_curve.sql
-- Start Time
-- SELECT MIN(call_block_time) FROM curvefi_optimism.StableSwap_call_coins
{% set project_start_date = '2022-01-17' %}

WITH
  base_pools AS (
    --Need all base pools because the meta pools reference them
    SELECT
      arg0 AS tokenid,
      output_0 AS token,
      contract_address AS pool
    FROM {{ source('curvefi_optimism', 'StableSwap_call_coins') }}
    WHERE
      call_success
      and output_0 is not null
    GROUP BY
      arg0,
      output_0,
      contract_address --unique
  ),
  meta_pools AS (
    -- Meta Pools are "Base Pools" + 1 extra token (i.e. sUSD + 3pool = sUSD Metapool)
    SELECT
      tokenid,
      token,
      et.contract_address AS pool
    FROM
      (
        SELECT
          mp.evt_tx_hash,
          (bp.tokenid + 1) AS tokenid,
          bp.token,
          mp.evt_block_number
        FROM {{ source('curvefi_optimism', 'PoolFactory_evt_MetaPoolDeployed') }} mp
          INNER JOIN base_pools bp ON mp.base_pool = bp.pool
        GROUP BY
          mp.evt_tx_hash,
          (bp.tokenid + 1),
          bp.token,
          mp.evt_block_number --unique
        UNION ALL
        SELECT
          mp.evt_tx_hash,
          UINT256 '0' AS tokenid,
          mp.coin AS token,
          mp.evt_block_number
        FROM {{ source('curvefi_optimism', 'PoolFactory_evt_MetaPoolDeployed') }} mp
          INNER JOIN base_pools bp ON mp.base_pool = bp.pool
        GROUP BY
          mp.evt_tx_hash,
          mp.coin,
          mp.evt_block_number --unique
      ) mps
      -- the exchange address appears as an erc20 minted to itself (not in the deploymeny event)
    INNER JOIN {{ source('erc20_optimism','evt_transfer') }} et
      ON et.evt_tx_hash = mps.evt_tx_hash
      AND et."from" = 0x0000000000000000000000000000000000000000
      AND et.to = et.contract_address
      AND et.evt_block_number = mps.evt_block_number
      AND et.evt_block_time >= TIMESTAMP '2022-01-17'
    GROUP BY
      tokenid,
      token,
      et.contract_address --unique
  ),
  basic_pools AS (
    SELECT
      CAST(pos as UINT256) as tokenid,
      col AS token,
      pool
    FROM
      (
        SELECT
          pos - 1 as pos,
          col,
          output_0 AS pool
        FROM {{ source('curvefi_optimism', 'PoolFactory_call_deploy_plain_pool') }}
          CROSS JOIN UNNEST (_coins)
        WITH
          ORDINALITY AS coin_elements (col, pos)
        WHERE
          call_success
      ) a
    GROUP BY
      pos,
      col,
      pool --unique
  )
  -- handle for wsteth & pools that we're not deployed via the factory.. weird
  -- info from contract reads 'coins' https://optimistic.etherscan.io/address/0xb90b9b1f91a01ea22a182cd84c1e22222e39b415#readContract
  -- TODO/TOLINK: Query to check for Curve Pools with swap events that aren't mapped here
,
  custom_pools AS (
    SELECT
      version,
      tokenid,
      token,
      pool
    FROM
      (
        values
          --wstETH/ETH
          (
            'Basic Pool',
            0,
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee,
            0xb90b9b1f91a01ea22a182cd84c1e22222e39b415
          ),
          (
            'Basic Pool',
            1,
            0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb,
            0xb90b9b1f91a01ea22a182cd84c1e22222e39b415
          )
          --aPool
,
          (
            'Basic Pool',
            0,
            0x82E64f49Ed5EC1bC6e43DAD4FC8Af9bb3A2312EE,
            0x66b5792ed50a2a7405ea75c4b6b1913ef4e46661
          ),
          (
            'Basic Pool',
            1,
            0x625E7708f30cA75bfd92586e17077590C60eb4cD,
            0x66b5792ed50a2a7405ea75c4b6b1913ef4e46661
          ),
          (
            'Basic Pool',
            2,
            0x6ab707Aca953eDAeFBc4fD23bA73294241490620,
            0x66b5792ed50a2a7405ea75c4b6b1913ef4e46661
          )
          --sUSD/FRAX
,
          (
            'Basic Pool',
            0,
            0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9,
            0x54dcfe120d608551f9010d3b66620d230fd5c11b
          ),
          (
            'Basic Pool',
            1,
            0x29A3d66B30Bc4AD674A4FDAF27578B64f6afbFe7,
            0x54dcfe120d608551f9010d3b66620d230fd5c11b
          )
      ) a (version, tokenid, token, pool)
  ),
  agg_pools AS (
    SELECT
      version,
      tokenid,
      token,
      pool
    FROM
      (
        SELECT
          'Base Pool' AS version,
          tokenid,
          token,
          pool
        FROM
          base_pools
        UNION ALL
        SELECT
          'Meta Pool' AS version,
          tokenid,
          token,
          pool
        FROM
          meta_pools
        UNION ALL
        SELECT
          'Basic Pool' AS version,
          tokenid,
          token,
          pool
        FROM
          basic_pools
      ) a
    GROUP BY
      version,
      tokenid,
      token,
      pool --unique
  )
SELECT
  version,
  CAST(tokenid as INT256) as tokenid,
  token,
  pool
FROM
  agg_pools
UNION ALL
SELECT
  version,
  CAST(tokenid as INT256) as tokenid,
  token,
  pool
FROM
  custom_pools
WHERE
  pool NOT IN (
    SELECT
      pool
    FROM
      agg_pools
  ) --avoid dupes that are caught by factories