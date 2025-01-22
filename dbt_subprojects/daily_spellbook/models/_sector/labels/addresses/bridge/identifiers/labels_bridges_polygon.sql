{{config(
        alias = 'bridges_polygon',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["rantum"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    ('polygon', 0xBBbD1BbB4f9b936C3604906D7592A644071dE884, 'Allbridge: Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xF78765bd14B4E8527d9E4E5c5a5c11A44ad12F47, 'Biconomy: Hyphen Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x88DCDC47D2f83a99CF0000FDF667A468bB958a78, 'Celer Network: cBridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xa251c4691C1ffd7d9b128874C023427513D8Ac5C, 'Celer Network: cBridge 2.0', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xdEF78a28c78A461598d948bc0c689ce88f812AD8, 'Cerby: Bridge Fees Wallet', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xEf038429e3BAaF784e1DE93075070df2A43D4278, 'Cerby: Cross-Chain Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x43dE2d77BF8027e25dBD179B491e8d64f38398aA, 'deBridgeGate', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x29494C1673903e608352020CF8F545af70111Ad4, 'DeFi Basket: AaveV2 Deposit Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x4789499ed6D3c9E9b874E7E02AB8139779A51704, 'DeFi Basket: Autofarm Deposit Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x89Db516B50819593f058040F781BfF9880ca81a8, 'DeFi Basket: Quickswap Liquidity Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x840eA78f667B73853F4baEcE4a5EBe212C4039C1, 'Evodefi: Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xDF774A4F3EA5095535f5B8f5b9149caF90FF75Bd, 'Gains Network: Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xa33f7069f075A54481868e4C0b8D26925A218362, 'Gains Network: Locking Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xc315239cFb05F1E130E7E28E603CEa4C014c57f0, 'Hop Protocol: Ethereum Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x25D8039bB044dC227f741a9e381CA4cEAE2E6aE8, 'Hop Protocol: USDC Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x82a079DE7D2a8b59Bc932Df079eF5aA31B01AEB6, 'iSwap: Polygon Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x3c2269811836af69497E5F486A85D7316753cf62, 'LayerZero: Polygon Endpoint', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x99783B38eC79ff29234748b42c82Dc27A434a096, 'Less Network: Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xac0Cb30cFB91dd2Cbb7C12FbfC069b3f2332AD16, 'MacaronSwap: Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x05b70fb5477a93be33822bfb31fdaf2c171970df, 'Mayan: Swap Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x25864a712c80d33ba1ad7c23cffa18b46f2fc00c, 'Multichain: Fantom Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x6fF0609046A38D76Bd40C5863b4D1a2dCe687f73, 'Multichain: Router V6', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x3a5846882C0d5F8B0FA4bB04dc90C013104d125d, 'Optics: ERC-20 Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xa489b8981ae5652C9Dd6515848cB8Dbecae5E1B0, 'Optics: ETH Helper', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xec52a30e4bfe2d6b0ba1d0dbf78f265c0a119286, 'Rubic Exchange', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x8f5bbb2bb8c2ee94639e55d5f41de9b4839c1280, 'Synapse: Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xeC52A30E4bFe2D6B0ba1D0dbf78f265c0a119286, 'Wormhole: Polygon Core Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x1c6aE197fF4BF7BA96c66C5FD64Cb22450aF9cC8, 'Wormhole: Token Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x90BBd86a6Fe93D3bc3ed6335935447E75fAb7fCf, 'xPollinate: Transaction Manager', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x5a58505a96D1dbf8dF91cB21B54419FC36e93fdE, 'xPollinate: Transaction Manager 2', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x6090De2EC76eb1Dc3B5d632734415c93c44Fd113, 'Zeroswap: Bridge', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xbb256f544b8087596E8E6cdd7fE9726cC98CB400, 'ZigZag Exchange', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x45a01e4e04f14f7a4a6702c74187c5f6222033cd, 'Stargate', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x9d1b1669c73b033dfe47ae5a0164ab96df25b944, 'Stargate', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0x9295ee1d8C5b022Be115A2AD3c30C72E34e7F096, 'Across', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xce16f69375520ab01377ce7b88f5ba8c48f8d666, 'Squid', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier'),
    ('polygon', 0xd5f0f8db993d26f5df89e70a83d32b369dccdaa0, 'Symbiosis', 'bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_polygon', 'identifier')



    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)