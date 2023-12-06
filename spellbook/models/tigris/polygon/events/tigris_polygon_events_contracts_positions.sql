{{ config(
    
        alias = 'events_contracts_positions'
        )
}}

WITH 

hardcoded_positions as ( -- harcoding the position contracts since there's no event logged to get the position contract of the trading contracts
        SELECT 
            trading_contract, 
            positions_contract,
            trading_contract_version,
            blockchain
        FROM (
        VALUES 
        -- v1 trading contracts 
            (0x2381e421ee2a89ea627f971e8fdfa4ffa81c2cdd, 0x683fad69622a6237a1b08743edff7d9d4c7080f7, 'v1.1', 'polygon'),
            (0x311921e7d079ffc65f5d458f972c377559d70bbd, 0x683fad69622a6237a1b08743edff7d9d4c7080f7, 'v1.2', 'polygon'),
            (0x38889A19893eD9129a2f017a3F60ecbED6DBE5aA, 0x683fad69622a6237a1b08743edff7d9d4c7080f7, 'v1.3', 'polygon'),
            (0xb7260e90181cb2df86e61d614c44b30721cc6531, 0x683fad69622a6237a1b08743edff7d9d4c7080f7, 'v1.4', 'polygon'),
            (0x591a4e2adba199bdb08f28d00a1756f4c245bdf7, 0xbb323fe012bfa1728af14941d09e479612b64281, 'v1.5', 'polygon'),
            (0xb173fde7b7d419514d8b3f8e854e978ea93b1c50, 0xbb323fe012bfa1728af14941d09e479612b64281, 'v1.6', 'polygon'),
            (0xcde587e333327fbf887548b3eaf111fb50d38388, 0xbb323fe012bfa1728af14941d09e479612b64281, 'v1.7', 'polygon'),
            (0x28c5d4416f6cf0fc5f557067b54bd67a43fcc98f, 0xbb323fe012bfa1728af14941d09e479612b64281, 'v1.8', 'polygon'),
        -- v2 trading contracts 
            (0x1E17288b6BEc5c432D0ab7BB16Fe37C0D094a67d, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.1', 'polygon'),
            (0x67D11fF59a7797bD5E3d58510d7C086eA0732436, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.2', 'polygon'),
            (0x486bafa7D418896C67dCCAB0612589f208aa3249, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.3', 'polygon'),
            (0xa80aa05437112BA844628eE73AcfE94c31f8fe28, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.4', 'polygon'),
            (0x278a29098ef8c2af6b948d079c32b54188b618f0, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.5', 'polygon'),
            (0xA35eabB4be62Ed07E88c2aF73234fe7dD48a73D4, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.5', 'polygon'),
        -- v2 options contracts 
            (0x28969ded75cf3bce9f2b6bd49ac92d8ba8dfc3d1, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.1', 'polygon'),
            (0x87e0df5ac8a657af9f1472995354a09a4f9c381a, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.2', 'polygon'),
            (0xfeabec2cac8a1a2f1c0c181572aa88c8b91288b2, 0xb60F2011d30b5b901d55a701C58f63aB34b4C23f, 'v2.3', 'polygon')
        ) as temp_table (trading_contract, positions_contract, trading_contract_version, blockchain)
)

SELECT * FROM hardcoded_positions