 {{
  config(
        alias='vaults',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "nftx",
                                    \'["hildobby"]\') }}')
}}

WITH pools AS (
    SELECT vault_id
    , array_agg(distinct pool) AS pools
    FROM (
        SELECT vaultId AS vault_id
        , pool
        FROM {{source('nftx_v2_ethereum','Staking_evt_PoolCreated')}}
        
        UNION ALL
        
        SELECT vaultId AS vault_id
        , pool
        FROM {{source('nftx_v2_ethereum','Staking_evt_PoolUpdated')}}
        )
    GROUP BY 1
    )

SELECT CAST(v.vaultId AS double) AS vault_id
, cv.name
, cv.symbol
, p.pools
, v.assetAddress AS asset_address
, v.vaultAddress AS vault_address
, v.evt_block_time AS block_time
, v.evt_block_number AS block_number
, v.contract_address AS project_contract_address
FROM {{source('nftx_v2_ethereum','NFTXVaultFactoryUpgradeable_v1_evt_NewVault')}} v
INNER JOIN {{source('nftx_v2_ethereum','NFTXVaultFactoryUpgradeable_v1_call_createVault')}} cv ON cv.call_success
    AND cv.output_0=v.vaultId
LEFT JOIN pools p ON p.vault_id=v.vaultId