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
    FROM (VALUES ('0xd8da60bDe22461D7Aa11540C338dC56a0E546b0D', 'barn', 'Legacy'),
                 ('0xe66EB17F8679f90cCc0ed9A05c23f267cAef421E', 'barn', 'Naive'),
                 ('0x79f175703ECE2AbC7BE2e2a603eBBa319FB84Acc', 'barn', 'Baseline'),
                 ('0x508bCC23C1a808A9c41D10E2FCB544Ffb76ae3E5', 'barn', 'MIP'),
                 ('0x7942a2b3540d1eC40B2740896f87aEcB2a588731', 'barn', 'Quasimodo'),
                 ('0x26029B63C7DBD0C4C04D7226C3e7de5EAb3DB3D8', 'barn', 'Gnosis_1Inch'),
                 ('0x52ac5B5e85De9aa72ef5925989Fc419AA04EB15b', 'barn', 'SeaSolver'),
                 ('0xf794f31976a9a1d866cadfecde8984e656395a70', 'barn', 'Quasimodo'),
                 ('0x449944c987d622cd8db9c150fd4ecdfe4435b836', 'barn', 'Naive'),
                 ('0x9aaceb30c5b0e676a6b20d0c6be68f58bc7d8659', 'barn', 'Baseline'),
                 ('0xd474668a7a9daf34f37c54b22622106277c24166', 'barn', 'MIP'),
                 ('0x97d69672e8fe5be64d7d5bbba438ae9b08187667', 'barn', 'MIP'),
                 ('0x14Cda0a87a3E98e704a40D586cc7cF0889523f31', 'prod', 'Legacy'),
                 ('0x230FF84887616f29F5c55Ce68FF627a29f79D0cC', 'prod', 'Naive'),
                 ('0xB4783aBc7B1e5FdAEb36F78eE585e03Ee6eBB718', 'prod', 'Baseline'),
                 ('0x920c9D5ec65dAC83435e9aF378C0f6fac69b8B66', 'prod', 'MIP'),
                 ('0x7938A4770953Ab0003bF1e1fC5fC7F769B57d525', 'prod', 'Quasimodo'),
                 ('0xd2F50B092ec32623c4955cEF4AE30C4699353735', 'prod', 'Gnosis_1Inch'),
                 ('0x68dEE65bB88d919463495E5CeA9870a81f1e9413', 'service', 'Withdraw'),
                 ('0xa03be496e67ec29bc62f01a428683d7f9c204930', 'service', 'Withdraw'),
                 ('0x7524942F9283FBFa8F17b05CC0a9cBde397d25b3', 'test', 'Test 1')

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