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
    FROM (VALUES ('\xf2d21ad3c88170d4ae52bbbeba80cb6078d276f4'::bytea, 'prod', 'MIP'),
                 ('\x15f4c337122ec23859ec73bec00ab38445e45304'::bytea, 'prod', 'ParaSwap'),
                 ('\xde1c59bc25d806ad9ddcbe246c4b5e5505645718'::bytea, 'prod', '1inch'),
                 ('\x340185114f9d2617dc4c16088d0375d09fee9186'::bytea, 'prod', 'Naive'),
                 ('\x833f076d182123ca8dde2743045ea02957bd61ef'::bytea, 'prod', 'Baseline'),
                 ('\xe92f359e6f05564849afa933ce8f62b8007a1d5d'::bytea, 'prod', '0x'),
                 ('\x77ec2a722c2393d3fd64617bbaf1499c713e616b'::bytea, 'prod', 'QuasiModo'),
                 ('\xa6ddbd0de6b310819b49f680f65871bee85f517e'::bytea, 'prod', 'Legacy'),
                 ('\x2d15894fac906386ff7f4bd07fceac43fcf80c73'::bytea, 'prod', 'DexCowAgg'),
                 ('\xa97DEF4fBCBa3b646dd169Bc2eeE40f0f3fE7771'::bytea, 'prod', 'BalancerSOR'),
                 ('\x6fa201C3Aff9f1e4897Ed14c7326cF27548d9c35'::bytea, 'prod', 'Otex'),
                 ('\xe58c68679e7aab8ef83bf37e88b18eb1f6e30e22'::bytea, 'prod', 'PLM'),
                 ('\x0e8F282CE027f3ac83980E6020a2463F4C841264'::bytea, 'prod', 'Legacy'),
                 ('\x7A0A8890D71A4834285efDc1D18bb3828e765C6A'::bytea, 'prod', 'Naive'),
                 ('\x3Cee8C7d9B5C8F225A8c36E7d3514e1860309651'::bytea, 'prod', 'Baseline'),
                 ('\xe8fF24ec26bd46E0140d1824DA44eFad2a0920B5'::bytea, 'prod', 'MIP'),
                 ('\x731a0A8ab2C6FcaD841e82D06668Af7f18e34970'::bytea, 'prod', 'QuasiModo'),
                 ('\xb20B86C4e6DEEB432A22D773a221898bBBD03036'::bytea, 'prod', '1inch'),
                 ('\xE9aE2D792F981C53EA7f6493A17Abf5B2a45a86b'::bytea, 'prod', 'ParaSwap'),
                 ('\xdA869Be4AdEA17AD39E1DFeCe1bC92c02491504f'::bytea, 'prod', '0x'),
                 ('\x6D1247b8ACf4dFD5Ff8cfD6C47077ddC43d4500E'::bytea, 'prod', 'DexCowAgg'),
                 ('\xF7995B6B051166eA52218c37b8d03A2A6bbeF3DA'::bytea, 'prod', 'BalancerSOR'),
                 ('\xc9ec550BEA1C64D779124b23A26292cc223327b6'::bytea, 'prod', 'Otex'),
                 ('\x149d0f9282333681Ee41D30589824b2798E9fb47'::bytea, 'prod', 'PLM'),
                 ('\xe18B5632DF2Ec339228DD65e4D9F004eF59653d3'::bytea, 'prod', 'Atlas'),
                 ('\x8567351D6989d83513D3BC3ad951CcCe363941e3'::bytea, 'barn', 'Atlas'),
                 ('\x109BF9E0287Cc95cc623FBE7380dD841d4bdEb03'::bytea, 'barn', 'Otex'),
                 ('\x70f3c870b6e7e1d566e40c41e2e3d6e895fcee23'::bytea, 'barn', 'QuasiModo'),
                 ('\x97dd6a023b06ba4722aF8Af775ec3C2361e66684'::bytea, 'barn', '0x'),
                 ('\x6372bcbf66656e91b9213b61d861b5e815296207'::bytea, 'barn', 'ParaSwap'),
                 ('\x158261d17d2983b2cbaecc1ae71a34617ae4cb16'::bytea, 'barn', 'MIP'),
                 ('\x8c9d33828dace1eb9fc533ffde88c4a9db115061'::bytea, 'barn', '1inch'),
                 ('\xbfaf2b5e351586551d8bf461ba5b2b5455b173da'::bytea, 'barn', 'Baseline'),
                 ('\xb8650702412d0aa7f01f6bee70335a18d6a78e77'::bytea, 'barn', 'Naive'),
                 ('\x583cD88b9D7926357FE6bddF0E8950557fcDA0Ca'::bytea, 'barn', 'DexCowAgg'),
                 ('\x6c2999b6b1fad608ecea71b926d68ee6c62beef8'::bytea, 'barn', 'Legacy'),
                 ('\xED94b86275447e28ddbDd17BBEB1f62D607b5119'::bytea, 'barn', 'Legacy'),
                 ('\x8ccc61dba297833dbe5d95fd6360f106b9a7576e'::bytea, 'barn', 'Naive'),
                 ('\x0d2584da2f637805071f184bcfa1268ebae8a24a'::bytea, 'barn', 'Baseline'),
                 ('\xa0044c620da7f2876da7004719b8370eb7be5e50'::bytea, 'barn', 'MIP'),
                 ('\xda324c2f06d3544e7965767ce9ca536dcb67a660'::bytea, 'barn', 'QuasiModo'),
                 ('\xe33062a24149f7801a48b2675ed5111d3278f0f5'::bytea, 'barn', '1inch'),
                 ('\x080a8b1e2f3695e179453c5e617b72a381be44b9'::bytea, 'barn', 'ParaSwap'),
                 ('\xde786877a10dbb7eba25a4da65aecf47654f08ab'::bytea, 'barn', '0x'),
                 ('\xdae69affe582d36f330ee1145995a53fab670962'::bytea, 'barn', 'DexCowAgg'),
                 ('\x0b78e29ee55aa73b366730cf512d65c514eeb196'::bytea, 'barn', 'BalancerSOR'),
                 ('\x22dee0935c77d32c7241362b14e76fc2d5ef657d'::bytea, 'barn', 'BalancerSOR'),
                 ('\x5b0bfe439ab45a4f002c259b1654ed21c9a42d69'::bytea, 'barn', 'PLM'),
                 ('\x0798540ee03a8c2e68cef19c56d1faa86271d5cf'::bytea, 'service', 'Withdraw'),
                 ('\xdf30c9502eafea21ecc8410108dda338dd5047c5'::bytea, 'service', 'Withdraw'),
                 ('\x256bb5ad3dbdf61ae08d7cbc0b9223ccb1c60aae'::bytea, 'service', 'Withdraw'),
                 ('\x84e5c8518c248de590d5302fd7c32d2ae6b0123c'::bytea, 'service', 'Withdraw'),
                 ('\xa03be496e67ec29bc62f01a428683d7f9c204930'::bytea, 'service', 'Withdraw'),
                 ('\x2caef7f0ee82fb0abf1ab0dcd3a093803002e705'::bytea, 'test', 'Test Solver 1'),
                 ('\x56d4ed5e49539ebb1366c7d6b8f2530f1e4fe753'::bytea, 'test', 'Test Solver 2')
         ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued' end      as name,
       active
from registered_solvers
         left outer join known_solver_metadata
                         on solver = address;

CREATE UNIQUE INDEX IF NOT EXISTS view_solvers_address_unique_idx ON gnosis_protocol_v2.view_solvers (address);

COMMIT;

-- -- This job updates the view every half day to capture any new (but currently uncatalogued solvers)
-- INSERT INTO cron.job (schedule, command)
-- VALUES ('0 */12 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_solvers')
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;