BEGIN;
DROP MATERIALIZED VIEW IF EXISTS gnosis_protocol_v2.view_solvers;

CREATE MATERIALIZED VIEW gnosis_protocol_v2.view_solvers (address, environment, active, name) AS
WITH known_solvers (address, environment, active, name) as (
    SELECT *
    FROM (VALUES (decode('f2d21ad3c88170d4ae52bbbeba80cb6078d276f4', 'hex'), 'prod', True, 'MIP'),
                 (decode('15f4c337122ec23859ec73bec00ab38445e45304', 'hex'), 'prod', True, 'ParaSwap'),
                 (decode('de1c59bc25d806ad9ddcbe246c4b5e5505645718', 'hex'), 'prod', True, '1inch'),
                 (decode('340185114f9d2617dc4c16088d0375d09fee9186', 'hex'), 'prod', True, 'Naive'),
                 (decode('833f076d182123ca8dde2743045ea02957bd61ef', 'hex'), 'prod', True, 'Baseline'),
                 (decode('e92f359e6f05564849afa933ce8f62b8007a1d5d', 'hex'), 'prod', True, '0x'),
                 (decode('77ec2a722c2393d3fd64617bbaf1499c713e616b', 'hex'), 'prod', True, 'QuasiModo'),
                 (decode('a6ddbd0de6b310819b49f680f65871bee85f517e', 'hex'), 'prod', True, 'Legacy'),
                 (decode('2d15894fac906386ff7f4bd07fceac43fcf80c73', 'hex'), 'prod', True, 'DexCowAgg'),
                 (decode('a97DEF4fBCBa3b646dd169Bc2eeE40f0f3fE7771', 'hex'), 'prod', True, 'BalancerSOR'),
                 (decode('70f3c870b6e7e1d566e40c41e2e3d6e895fcee23', 'hex'), 'barn', False, 'QuasiModo'),
                 (decode('97dd6a023b06ba4722aF8Af775ec3C2361e66684', 'hex'), 'barn', False, '0x'),
                 (decode('6372bcbf66656e91b9213b61d861b5e815296207', 'hex'), 'barn', False, 'ParaSwap'),
                 (decode('158261d17d2983b2cbaecc1ae71a34617ae4cb16', 'hex'), 'barn', False, 'MIP'),
                 (decode('8c9d33828dace1eb9fc533ffde88c4a9db115061', 'hex'), 'barn', False, '1inch'),
                 (decode('bfaf2b5e351586551d8bf461ba5b2b5455b173da', 'hex'), 'barn', False, 'Baseline'),
                 (decode('b8650702412d0aa7f01f6bee70335a18d6a78e77', 'hex'), 'barn', False, 'Naive'),
                 (decode('583cD88b9D7926357FE6bddF0E8950557fcDA0Ca', 'hex'), 'barn', False, 'DexCowAgg'),
                 (decode('6c2999b6b1fad608ecea71b926d68ee6c62beef8', 'hex'), 'barn', False, 'Legacy'),
                 (decode('ED94b86275447e28ddbDd17BBEB1f62D607b5119', 'hex'), 'barn', True, 'Legacy'),
                 (decode('8ccc61dba297833dbe5d95fd6360f106b9a7576e', 'hex'), 'barn', True, 'Naive'),
                 (decode('0d2584da2f637805071f184bcfa1268ebae8a24a', 'hex'), 'barn', True, 'Baseline'),
                 (decode('a0044c620da7f2876da7004719b8370eb7be5e50', 'hex'), 'barn', True, 'MIP'),
                 (decode('da324c2f06d3544e7965767ce9ca536dcb67a660', 'hex'), 'barn', True, 'Quasimodo'),
                 (decode('e33062a24149f7801a48b2675ed5111d3278f0f5', 'hex'), 'barn', True, '1Inch'),
                 (decode('080a8b1e2f3695e179453c5e617b72a381be44b9', 'hex'), 'barn', True, 'ParaSwap'),
                 (decode('de786877a10dbb7eba25a4da65aecf47654f08ab', 'hex'), 'barn', True, '0x'),
                 (decode('dae69affe582d36f330ee1145995a53fab670962', 'hex'), 'barn', True, 'CowDexAg'),
                 (decode('22dee0935c77d32c7241362b14e76fc2d5ef657d', 'hex'), 'barn', True, 'BalancerSOR'),
                 (decode('0798540ee03a8c2e68cef19c56d1faa86271d5cf', 'hex'), 'service', False, 'Withdraw'),
                 (decode('df30c9502eafea21ecc8410108dda338dd5047c5', 'hex'), 'service', False, 'Withdraw'),
                 (decode('256bb5ad3dbdf61ae08d7cbc0b9223ccb1c60aae', 'hex'), 'service', False, 'Withdraw'),
                 (decode('84e5c8518c248de590d5302fd7c32d2ae6b0123c', 'hex'), 'service', True, 'Withdraw'),
                 (decode('2caef7f0ee82fb0abf1ab0dcd3a093803002e705', 'hex'), 'test', True, 'Test Solver 1'),
                 (decode('56d4ed5e49539ebb1366c7d6b8f2530f1e4fe753', 'hex'), 'test', True, 'Test Solver 2')
         ) as _
),

     unknown_solvers as (
         select distinct(solver), 'new', True, 'Uncatalogued'
         from gnosis_protocol_v2."GPv2Settlement_evt_Settlement"
         where solver not in (select address from known_solvers)
     )

SELECT *
FROM (
         select *
         from known_solvers
         union
         select *
         from unknown_solvers
     ) as _;

CREATE UNIQUE INDEX IF NOT EXISTS view_solvers_address_unique_idx ON gnosis_protocol_v2.view_solvers (address);

INSERT INTO cron.job (schedule, command)
VALUES ('0 */12 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_solvers')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
