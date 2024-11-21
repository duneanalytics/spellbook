{{ config(
    
    schema = 'aztec_v2_ethereum',
    alias = 'deposit_assets',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}')
}}

WITH 

assets_added as (
        SELECT
            UINT256 '0' as asset_id,
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as asset_address,
            null as asset_gas_limit,
            null as date_added

        UNION
        
        SELECT 
            assetId as asset_id,
            assetAddress as asset_address,
            assetGasLimit as asset_gas_limit,
            evt_block_time as date_added
        FROM 
        {{source('aztec_v2_ethereum', 'RollupProcessor_evt_AssetAdded')}}
)

SELECT 
    a.*,
    t.symbol,
    t.decimals
FROM 
assets_added a
LEFT JOIN
{{ source('tokens', 'erc20') }} t
    ON a.asset_address = t.contract_address
    AND t.blockchain = 'ethereum'