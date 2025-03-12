{{ config(
    schema = 'ovm1_optimism',
    alias = 'synthetix_genesis_contracts',
    materialized = 'view'
    )
}}

SELECT
    contract_address
    , contract_name
FROM (
    VALUES
    (0x0b3a73ee0740b3130e40b2a6b5aaf59e7e3ef74c, 'Math')
    ,(0x95a6a3f44a70172e7d50a9e28c85dfd712756b8c, 'AddressResolver')
    ,(0x1cb059b7e74fd21665968c908806143e744d5f30, 'ReadProxyAddressResolver')
    ,(0x47649022380d182da8010ae5d257fea4227b21ff, 'FlexibleStorage')
    ,(0x8454190c164e52664af2c9c24ab58c4e14d6bbe4, 'SystemSettings')
    ,(0x357b58e0b1be9d8a944380048fa1080c57c7a362, 'SystemStatus')
    ,(0x631e93a0fb06b5ec6d52c0a2d89a3f9672d6ba64, 'ExchangeRates')
    ,(0xd32138018210eda0028240638f35b70ecc0d8c22, 'RewardEscrow')
    ,(0x47ee58801c1ac44e54ff2651ae50525c5cfc66d0, 'RewardEscrowV2')
    ,(0x06c6d063896ac733673c4474e44d9268f2402a55, 'SynthetixEscrow')
    ,(0x71d838995f8a97f636cbfdcd8de94b30d2bd4760, 'SynthetixState')
    ,(0x4a16a42407aa491564643e1dfc1fd50af29794ef, 'ProxyFeePool')
    ,(0x02f7fb66b55e6ca476d126d96f14c5732eeb4363, 'DelegateApprovalsEternalStorage')
    ,(0x2a23bc0ea97a89abd91214e8e4d20f02fe14743f, 'DelegateApprovals')
    ,(0x14e6f8e6da00a32c069b11b64e48ea1fef2361d4, 'Liquidations')
    ,(0x76d2de36936005a0182a1bb61da501a8a044d477, 'EternalStorageLiquidations')
    ,(0x41140bf6498a36f2e44efd49f21dae3bbb7367c8, 'FeePoolEternalStorage')
    ,(0x608457bc647e6f8f76ead8b99d343d367e0f4d62, 'FeePool')
    ,(0xd12a749e2ff15e66e095d0bfd3ce680756f36379, 'FeePoolState')
    ,(0x5d9187630e99dbce4bcab8733b76757f7f44aa2e, 'RewardsDistribution')
    ,(0x8700daec35af8ff88c16bdf0418774cb3d7599b4, 'ProxyERC20')
    ,(0xb9c6ca25452e7f6d0d3340ce1e9b573421afc2ee, 'TokenStateSynthetix')
    ,(0xd85eafa37734e4ad237c3a3443d64dc94ae998e7, 'Synthetix')
    ,(0x218067172e9e0460a883458d44bd1f56ea609502, 'ProxySynthetix')
    ,(0x5a528e35165e19f3392c9631243dd04d1229d324, 'DebtCache')
    ,(0xe318e4618e5684668992935d7231cb837a44e670, 'Exchanger')
    ,(0x7ef87c14f50cffe2e73d2c87916c3128c56593a8, 'ExchangeState')
    ,(0x8377b25b8564f6be579865639776c5082cb37163, 'Issuer')
    ,(0x2dcad1a019fba8301b77810ae14007cc88ed004b, 'TradingRewards')
    ,(0xcdb7d0a946223255d39a6e29b54f08f3291cc118, 'EscrowChecker')
    ,(0x92bac115d89ca17fd02ed9357ceca32842acb4c2, 'TokenStatesUSD')
    ,(0xbecc58c6d7ca71b6fcc4cc8c9c5294a0ea7a0397, 'ProxysUSD')
    ,(0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9, 'ProxyERC20sUSD')
    ,(0xf2ff43da7b6e5963059b7004df43b5c5870eeb18, 'SynthsUSD')
    ,(0xc8e91c926e04be1cb94e51c5379d14774d51ae6c, 'EtherCollateral')
    ,(0xc0c66470e766ae2026e6695966c56c90741811aa, 'EtherCollateralsUSD')
    ,(0x4d7186818dabfe88bd80421656bbd07dffc979cc, 'SynthetixBridgeToBase')
    ,(0x70b21b422dade467659522892a857f0ee31cebb4, 'CollateralManager')
    ,(0x87b1481c82913301fc6c884ac266a7c430f92cfa, 'SynthUtil')
    ,(0x54581a23f62d147ac76d454f0b3ef77f9d766058, 'DappMaintenance')
    ,(0xad32aa4bff8b61b4ae07e3ba437cf81100af0cd7, 'Wrapper')
    ,(0x8a91e92fdd86e734781c38db52a390e1b99fba7c, 'Wrapper')
) AS x (contract_address, contract_name)