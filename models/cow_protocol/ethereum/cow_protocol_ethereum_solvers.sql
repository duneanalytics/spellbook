{{ config(alias='solvers',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha"]\') }}'
)}}

-- Find the PoC Query here: https://dune.com/queries/1276806
WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_ethereum', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_ethereum', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
),
-- Sorting by (evt_block_number, evt_index) allows us to pick the most recent activation status of each unique solver
ranked_solver_events as (
    select
        rank() over (partition by solver order by evt_block_number desc, evt_index desc) as rk,
        solver,
        evt_block_number,
        evt_index,
        activated as active
    from solver_activation_events
),
registered_solvers as (
    select solver, active
    from ranked_solver_events
    where rk = 1
),
-- Manually inserting environment and name for each "known" solver
known_solver_metadata (address, environment, name) as (
    SELECT *
    FROM (VALUES ('0xf2d21ad3c88170d4ae52bbbeba80cb6078d276f4', 'prod', 'MIP'),
                 ('0x15f4c337122ec23859ec73bec00ab38445e45304', 'prod', 'Gnosis_ParaSwap'),
                 ('0xde1c59bc25d806ad9ddcbe246c4b5e5505645718', 'prod', 'Gnosis_1inch'),
                 ('0x340185114f9d2617dc4c16088d0375d09fee9186', 'prod', 'Naive'),
                 ('0x833f076d182123ca8dde2743045ea02957bd61ef', 'prod', 'Baseline'),
                 ('0xe92f359e6f05564849afa933ce8f62b8007a1d5d', 'prod', 'Gnosis_0x'),
                 ('0x77ec2a722c2393d3fd64617bbaf1499c713e616b', 'prod', 'QuasiModo'),
                 ('0xa6ddbd0de6b310819b49f680f65871bee85f517e', 'prod', 'Legacy'),
                 ('0x2d15894fac906386ff7f4bd07fceac43fcf80c73', 'prod', 'DexCowAgg'),
                 ('0xa97DEF4fBCBa3b646dd169Bc2eeE40f0f3fE7771', 'prod', 'Gnosis_BalancerSOR'),
                 ('0x6fa201C3Aff9f1e4897Ed14c7326cF27548d9c35', 'prod', 'Otex'),
                 ('0xe58c68679e7aab8ef83bf37e88b18eb1f6e30e22', 'prod', 'PLM'),
                 ('0x0e8F282CE027f3ac83980E6020a2463F4C841264', 'prod', 'Legacy'),
                 ('0x7A0A8890D71A4834285efDc1D18bb3828e765C6A', 'prod', 'Naive'),
                 ('0x3Cee8C7d9B5C8F225A8c36E7d3514e1860309651', 'prod', 'Baseline'),
                 ('0xe8fF24ec26bd46E0140d1824DA44eFad2a0920B5', 'prod', 'MIP'),
                 ('0x731a0A8ab2C6FcaD841e82D06668Af7f18e34970', 'prod', 'QuasiModo'),
                 ('0xb20B86C4e6DEEB432A22D773a221898bBBD03036', 'prod', 'Gnosis_1inch'),
                 ('0xE9aE2D792F981C53EA7f6493A17Abf5B2a45a86b', 'prod', 'Gnosis_ParaSwap'),
                 ('0xdA869Be4AdEA17AD39E1DFeCe1bC92c02491504f', 'prod', 'Gnosis_0x'),
                 ('0x6D1247b8ACf4dFD5Ff8cfD6C47077ddC43d4500E', 'prod', 'DexCowAgg'),
                 ('0xF7995B6B051166eA52218c37b8d03A2A6bbeF3DA', 'prod', 'Gnosis_BalancerSOR'),
                 ('0xc9ec550BEA1C64D779124b23A26292cc223327b6', 'prod', 'Otex'),
                 ('0x149d0f9282333681Ee41D30589824b2798E9fb47', 'prod', 'PLM'),
                 ('0xe18B5632DF2Ec339228DD65e4D9F004eF59653d3', 'prod', 'Atlas'),
                 ('0xA21740833858985e4D801533a808786d3647Fb83', 'prod', 'Laertes'),
                 ('0x398890BE7c4FAC5d766E1AEFFde44B2EE99F38EF', 'prod', 'Seasolver'),
                 ('0x97Ec0a17432D71a3234EF7173C6B48a2C0940896', 'prod', 'Quasilabs'),
                 ('0xF5181183D43796120a004130d0CaeE5B2DF2D441', 'prod', 'DMA'),
                 ('0xbff9a1b539516f9e20c7b621163e676949959a66', 'prod', 'Raven'),
                 ('0x55A37A2E5e5973510Ac9D9C723aeC213fA161919', 'prod', 'Barter'),
                 ('0x4F422556FaD14720B2691359fAd0C0F6B1B39113', 'prod', 'Naive'),
                 ('0x96a3B53197eA4941482b142F5036E6432086Da8a', 'prod', 'Baseline'),
                 ('0x8366812163991Dd7a54A0ec6191e5E5961Db845b', 'prod', 'Gnosis_1inch'),
                 ('0x7AC1E2F8593c096Da0A94B71b6f70ca81b6b86F0', 'prod', 'Gnosis_ParaSwap'),
                 ('0x8419FF1bCBCe6f0233cf3460B28c32b6951Fc457', 'prod', 'Gnosis_0x'),
                 ('0xc20e52dDaFB05c67D78A440f2eAe344EB230A3dA', 'prod', 'Gnosis_BalancerSOR'),
                 ('0x511d452b738b3f1aDA0E74e7A3412F5D975FC548', 'prod', 'Otex'),
                 ('0x84d143682764DD1f42C4de763262107BD90c4F42', 'prod', 'PLM'),
                 ('0x31A9Ec3A6E29039C74723E387DE42b79E6856FD8', 'prod', 'Laertes'),
                 ('0x43872b55A12E087935765611851E94e3f0a79249', 'prod', 'Seasolver'),
                 ('0x1e8D9a45175B2a4122F7827ce1eA3B08327b2ba0', 'prod', 'Quasilabs'),
                 ('0xD8b9c8e1a94baEAaf4D1CA2C45723eb88236130E', 'prod', 'Raven'),
                 ('0x0C60276BeaDc5BA35007661A89E0d5E7476523f8', 'prod', 'Barter'),
                 ('0x452d604f08affFc4E87E74e3279BdBdeCeD07232', 'prod', 'PropellerHeads'),
                 ('0x69f9365405762405cc17f7979aa8e94fd95c1e80', 'barn', 'Barter'),
                 ('0xFFC5E9d86c0e069f8B037c841ACc72cF94eeBaD8', 'barn', 'Barter'),
                 ('0x1857afb4da9bd4cc1c6e5287ad41cb5be469f14e', 'barn', 'Raven'),
                 ('0x5B2F5e5C94a5De698e2DeC7f30E90069eb3b12bb', 'barn', 'DMA'),
                 ('0x872A1B63A739190D0780721d57D8d92ef766Db35', 'barn', 'Quasilabs'),
                 ('0x8a4e90e9AFC809a69D2a3BDBE5fff17A12979609', 'barn', 'Seasolver'),
                 ('0x0a308697e1d3a91dcB1e915C51F8944AaEc9015F', 'barn', 'Laertes'),
                 ('0x8567351D6989d83513D3BC3ad951CcCe363941e3', 'barn', 'Atlas'),
                 ('0x109BF9E0287Cc95cc623FBE7380dD841d4bdEb03', 'barn', 'Otex'),
                 ('0x70f3c870b6e7e1d566e40c41e2e3d6e895fcee23', 'barn', 'QuasiModo'),
                 ('0x97dd6a023b06ba4722aF8Af775ec3C2361e66684', 'barn', 'Gnosis_0x'),
                 ('0x6372bcbf66656e91b9213b61d861b5e815296207', 'barn', 'Gnosis_ParaSwap'),
                 ('0x158261d17d2983b2cbaecc1ae71a34617ae4cb16', 'barn', 'MIP'),
                 ('0x8c9d33828dace1eb9fc533ffde88c4a9db115061', 'barn', 'Gnosis_1inch'),
                 ('0xbfaf2b5e351586551d8bf461ba5b2b5455b173da', 'barn', 'Baseline'),
                 ('0xb8650702412d0aa7f01f6bee70335a18d6a78e77', 'barn', 'Naive'),
                 ('0x583cD88b9D7926357FE6bddF0E8950557fcDA0Ca', 'barn', 'DexCowAgg'),
                 ('0x6c2999b6b1fad608ecea71b926d68ee6c62beef8', 'barn', 'Legacy'),
                 ('0xED94b86275447e28ddbDd17BBEB1f62D607b5119', 'barn', 'Legacy'),
                 ('0x8ccc61dba297833dbe5d95fd6360f106b9a7576e', 'barn', 'Naive'),
                 ('0x0d2584da2f637805071f184bcfa1268ebae8a24a', 'barn', 'Baseline'),
                 ('0xa0044c620da7f2876da7004719b8370eb7be5e50', 'barn', 'MIP'),
                 ('0xda324c2f06d3544e7965767ce9ca536dcb67a660', 'barn', 'QuasiModo'),
                 ('0xe33062a24149f7801a48b2675ed5111d3278f0f5', 'barn', 'Gnosis_1inch'),
                 ('0x080a8b1e2f3695e179453c5e617b72a381be44b9', 'barn', 'Gnosis_ParaSwap'),
                 ('0xde786877a10dbb7eba25a4da65aecf47654f08ab', 'barn', 'Gnosis_0x'),
                 ('0xdae69affe582d36f330ee1145995a53fab670962', 'barn', 'DexCowAgg'),
                 ('0x0b78e29ee55aa73b366730cf512d65c514eeb196', 'barn', 'Gnosis_BalancerSOR'),
                 ('0x22dee0935c77d32c7241362b14e76fc2d5ef657d', 'barn', 'Gnosis_BalancerSOR'),
                 ('0x5b0bfe439ab45a4f002c259b1654ed21c9a42d69', 'barn', 'PLM'),
                 ('0xC1624D29b82314Cd5cF52dEB293c87794bFAa9f0', 'barn', 'Legacy'),
                 ('0x9AF3E1C8257557E2D70074fa03317F1A11595d02', 'barn', 'Naive'),
                 ('0x542AAD0402F973a6FCFbf5d60dfC1b0C4233118c', 'barn', 'Baseline'),
                 ('0x340c0C9E87C1E1ae5677506412e988748d8417ce', 'barn', 'Gnosis_1inch'),
                 ('0x641830bD5E9F283e8f26c60846195e2201dFD09F', 'barn', 'Gnosis_ParaSwap'),
                 ('0xeF7D51bD026E60ca75c2155419F29C0d31a6611C', 'barn', 'Gnosis_0x'),
                 ('0x31Ac5E51a168B5179c703f3b05F120748C8c7c88', 'barn', 'Gnosis_BalancerSOR'),
                 ('0xe374f64513f2432BcbF964B5ab84bD350D1FF222', 'barn', 'Otex'),
                 ('0x3cdC01749eEEb4A26b1a9a9611328F232bE06be7', 'barn', 'PLM'),
                 ('0xDE478f29C9566499f741cBF91CB068F1C2614B69', 'barn', 'Laertes'),
                 ('0xD01BA5b3C4142F358EfFB4d6Cb44A11E31600330', 'barn', 'Seasolver'),
                 ('0xF330Ee61d01ef1AC9FFA64662263e5E2DE93bbdE', 'barn', 'Quasilabs'),
                 ('0x3C3513c88bD7A919Cb732F777Cd80cd773Beb011', 'barn', 'Raven'),
                 ('0xAC0128ACC5c15945aB4E81F09f58CF05ec1844Ed', 'barn', 'Barter'),
                 ('0xd3eEc78a89B04f95965927F408F07C13996B8378', 'barn', 'PropellerHeads'),
                 ('0x0798540ee03a8c2e68cef19c56d1faa86271d5cf', 'service', 'Withdraw'),
                 ('0xdf30c9502eafea21ecc8410108dda338dd5047c5', 'service', 'Withdraw'),
                 ('0x256bb5ad3dbdf61ae08d7cbc0b9223ccb1c60aae', 'service', 'Withdraw'),
                 ('0x84e5c8518c248de590d5302fd7c32d2ae6b0123c', 'service', 'Withdraw'),
                 ('0xa03be496e67ec29bc62f01a428683d7f9c204930', 'service', 'Withdraw'),
                 ('0x2caef7f0ee82fb0abf1ab0dcd3a093803002e705', 'test', 'Test Solver 1'),
                 ('0x56d4ed5e49539ebb1366c7d6b8f2530f1e4fe753', 'test', 'Test Solver 2')
         ) as _
)
-- Combining the metadata with current activation status for final table
select CAST(solver AS VARCHAR(42)) as address,
      case when environment is not null then environment else 'new' end as environment,
      case when name is not null then name else 'Uncatalogued' end      as name,
      active
from registered_solvers
    left outer join known_solver_metadata on CAST(solver AS VARCHAR(42)) = lower(address);
