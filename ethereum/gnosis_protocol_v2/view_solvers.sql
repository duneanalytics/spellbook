BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol_v2.view_solvers;

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
    FROM (VALUES (decode('f2d21ad3c88170d4ae52bbbeba80cb6078d276f4', 'hex'), 'prod', 'MIP'),
                 (decode('15f4c337122ec23859ec73bec00ab38445e45304', 'hex'), 'prod', 'ParaSwap'),
                 (decode('de1c59bc25d806ad9ddcbe246c4b5e5505645718', 'hex'), 'prod', '1inch'),
                 (decode('340185114f9d2617dc4c16088d0375d09fee9186', 'hex'), 'prod', 'Naive'),
                 (decode('833f076d182123ca8dde2743045ea02957bd61ef', 'hex'), 'prod', 'Baseline'),
                 (decode('e92f359e6f05564849afa933ce8f62b8007a1d5d', 'hex'), 'prod', '0x'),
                 (decode('77ec2a722c2393d3fd64617bbaf1499c713e616b', 'hex'), 'prod', 'QuasiModo'),
                 (decode('a6ddbd0de6b310819b49f680f65871bee85f517e', 'hex'), 'prod', 'Legacy'),
                 (decode('2d15894fac906386ff7f4bd07fceac43fcf80c73', 'hex'), 'prod', 'DexCowAgg'),
                 (decode('a97DEF4fBCBa3b646dd169Bc2eeE40f0f3fE7771', 'hex'), 'prod', 'BalancerSOR'),
                 (decode('70f3c870b6e7e1d566e40c41e2e3d6e895fcee23', 'hex'), 'barn', 'QuasiModo'),
                 (decode('97dd6a023b06ba4722aF8Af775ec3C2361e66684', 'hex'), 'barn', '0x'),
                 (decode('6372bcbf66656e91b9213b61d861b5e815296207', 'hex'), 'barn', 'ParaSwap'),
                 (decode('158261d17d2983b2cbaecc1ae71a34617ae4cb16', 'hex'), 'barn', 'MIP'),
                 (decode('8c9d33828dace1eb9fc533ffde88c4a9db115061', 'hex'), 'barn', '1inch'),
                 (decode('bfaf2b5e351586551d8bf461ba5b2b5455b173da', 'hex'), 'barn', 'Baseline'),
                 (decode('b8650702412d0aa7f01f6bee70335a18d6a78e77', 'hex'), 'barn', 'Naive'),
                 (decode('583cD88b9D7926357FE6bddF0E8950557fcDA0Ca', 'hex'), 'barn', 'DexCowAgg'),
                 (decode('6c2999b6b1fad608ecea71b926d68ee6c62beef8', 'hex'), 'barn', 'Legacy'),
                 (decode('ED94b86275447e28ddbDd17BBEB1f62D607b5119', 'hex'), 'barn', 'Legacy'),
                 (decode('8ccc61dba297833dbe5d95fd6360f106b9a7576e', 'hex'), 'barn', 'Naive'),
                 (decode('0d2584da2f637805071f184bcfa1268ebae8a24a', 'hex'), 'barn', 'Baseline'),
                 (decode('a0044c620da7f2876da7004719b8370eb7be5e50', 'hex'), 'barn', 'MIP'),
                 (decode('da324c2f06d3544e7965767ce9ca536dcb67a660', 'hex'), 'barn', 'Quasimodo'),
                 (decode('e33062a24149f7801a48b2675ed5111d3278f0f5', 'hex'), 'barn', '1Inch'),
                 (decode('080a8b1e2f3695e179453c5e617b72a381be44b9', 'hex'), 'barn', 'ParaSwap'),
                 (decode('de786877a10dbb7eba25a4da65aecf47654f08ab', 'hex'), 'barn', '0x'),
                 (decode('dae69affe582d36f330ee1145995a53fab670962', 'hex'), 'barn', 'CowDexAg'),
                 (decode('0b78e29ee55aa73b366730cf512d65c514eeb196', 'hex'), 'barn', 'BalancerSOR'),
                 (decode('22dee0935c77d32c7241362b14e76fc2d5ef657d', 'hex'), 'barn', 'BalancerSOR'),
                 (decode('0798540ee03a8c2e68cef19c56d1faa86271d5cf', 'hex'), 'service', 'Withdraw'),
                 (decode('df30c9502eafea21ecc8410108dda338dd5047c5', 'hex'), 'service', 'Withdraw'),
                 (decode('256bb5ad3dbdf61ae08d7cbc0b9223ccb1c60aae', 'hex'), 'service', 'Withdraw'),
                 (decode('84e5c8518c248de590d5302fd7c32d2ae6b0123c', 'hex'), 'service', 'Withdraw'),
                 (decode('a03be496e67ec29bc62f01a428683d7f9c204930', 'hex'), 'service', 'Withdraw'),
                 (decode('2caef7f0ee82fb0abf1ab0dcd3a093803002e705', 'hex'), 'test', 'Test Solver 1'),
                 (decode('56d4ed5e49539ebb1366c7d6b8f2530f1e4fe753', 'hex'), 'test', 'Test Solver 2')
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

-- This job updates the view every half day to capture any new (but currently uncatalogued solvers)
INSERT INTO cron.job (schedule, command)
VALUES ('0 */12 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_solvers')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
