BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol_v2.view_solvers CASCADE;
COMMIT;

BEGIN;
CREATE MATERIALIZED VIEW gnosis_protocol_v2.view_solvers (address, environment, name, active) AS
WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from gnosis_protocol_v2."GPv2AllowListAuthentication_evt_SolverAdded"
    union
    select solver, evt_block_number, evt_index, False as activated
    from gnosis_protocol_v2."GPv2AllowListAuthentication_evt_SolverRemoved"
),
-- Sorting by (evt_block_number, evt_index) allows us to pick the most recent activation status of each unique solver
registered_solvers as (
    select distinct on (solver) solver,
                                activated as active
    from solver_activation_events
    order by solver, evt_block_number desc, evt_index desc
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
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued' end      as name,
       active
from registered_solvers
         left outer join known_solver_metadata
                         on solver = replace(address, '0x', '\x')::bytea;

CREATE UNIQUE INDEX IF NOT EXISTS view_solvers_address_unique_idx ON gnosis_protocol_v2.view_solvers (address);

COMMIT;

-- This job updates the view every half day to capture any new (but currently uncatalogued solvers)
INSERT INTO cron.job (schedule, command)
VALUES ('0 */12 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_solvers')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;