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
                 (decode('a6ddbd0de6b310819b49f680f65871bee85f517e', 'hex'), 'prod', False, 'Legacy (Archived)'),
                 (decode('2d15894fac906386ff7f4bd07fceac43fcf80c73', 'hex'), 'prod', True, 'DexCowAgg'),
                 (decode('70f3c870b6e7e1d566e40c41e2e3d6e895fcee23', 'hex'), 'barn', True, 'QuasiModo'),
                 (decode('97dd6a023b06ba4722af8af775ec3c2361e66684', 'hex'), 'barn', True, '0x'),
                 (decode('6372bcbf66656e91b9213b61d861b5e815296207', 'hex'), 'barn', True, 'ParaSwap'),
                 (decode('158261d17d2983b2cbaecc1ae71a34617ae4cb16', 'hex'), 'barn', True, 'MIP'),
                 (decode('8c9d33828dace1eb9fc533ffde88c4a9db115061', 'hex'), 'barn', True, '1inch'),
                 (decode('bfaf2b5e351586551d8bf461ba5b2b5455b173da', 'hex'), 'barn', True, 'Baseline'),
                 (decode('b8650702412d0aa7f01f6bee70335a18d6a78e77', 'hex'), 'barn', True, 'Naive'),
                 (decode('583cD88b9D7926357FE6bddF0E8950557fcDA0Ca', 'hex'), 'barn', True, 'DexCowAgg'),
                 (decode('6c2999b6b1fad608ecea71b926d68ee6c62beef8', 'hex'), 'barn', False, 'Legacy (Archived)'),
                 (decode('0798540ee03a8c2e68cef19c56d1faa86271d5cf', 'hex'), 'service', False, 'Withdraw (Archived)'),
                 (decode('256bb5ad3dbdf61ae08d7cbc0b9223ccb1c60aae', 'hex'), 'service', True, 'Withdraw'),
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
