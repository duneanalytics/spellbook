{{ config(
    
    schema = 'aztec_v2_ethereum',
    alias = 'bridges',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}')
}}

WITH  

bridges_label (protocol, version, description, contract_address) as (
        VALUES 
            ('Aztec RollupProcessor', '1.0', 'Prod Aztec Rollup', 0xff1f2b4adb9df6fc8eafecdcbf96a2b351680455),
            ('Element', '1.0', 'Prod Element Bridge', 0xaed181779a8aabd8ce996949853fea442c2cdb47),
            ('Lido', '1.0', 'Prod Lido Bridge', 0x381abf150b53cc699f0dbbbef3c5c0d1fa4b3efd),
            ('AceofZk', '1.0', 'Ace Of ZK NFT - nonfunctional', 0x0eb7f9464060289fe4fddfde2258f518c6347a70),
            ('Curve', '1.0', 'CurveStEth Bridge', 0x0031130c56162e00a7e9c01ee4147b11cbac8776),
            ('Yearn', '1.0', 'Yearn Deposits', 0xe71a50a78cccff7e20d8349eed295f12f0c8c9ef),
            ('Aztec', '1.0', 'ERC4626 Tokenized Vault', 0x3578d6d5e1b4f07a48bb1c958cbfec135bef7d98),
            ('Curve', '1.0', 'CurveStEth Bridge V2', 0xe09801da4c74e62fb42dfc8303a1c1bd68073d1a),
            ('Uniswap', '1.0', 'UniswapDCABridge', 0x94679a39679ffe53b53b6a1187aa1c649a101321)
), 

bridges_creation as (
        SELECT 
            bridgeAddress, 
            'Bridge' as contract_type, 
            AVG(bridgeGasLimit) as blank -- to get unique bridges 
        FROM 
        {{source('aztec_v2_ethereum', 'RollupProcessor_evt_BridgeAdded')}}
        GROUP BY 1, 2 
        
        UNION 
        
        SELECT 
            0xFF1F2B4ADb9dF6FC8eAFecDcbF96A2B351680455 as bridgeAddress, 
            'Rollup' as contract_type,
            100 as blank 
)

SELECT 
    bl.protocol,
    bl.version,
    bl.description, 
    bc.contract_type,
    bc.bridgeAddress as contract_address 
FROM 
bridges_creation bc 
LEFT JOIN 
bridges_label bl 
    ON bl.contract_address = bc.bridgeAddress