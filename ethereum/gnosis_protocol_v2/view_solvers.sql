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
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued' end      as name,
       active
from registered_solvers
         left outer join gnosis_protocol_v2.solver_names
                         on solver = replace(address, '0x', '\x')::bytea;

CREATE UNIQUE INDEX IF NOT EXISTS view_solvers_address_unique_idx ON gnosis_protocol_v2.view_solvers (address);

COMMIT;

-- This job updates the view every half day to capture any new (but currently uncatalogued solvers)
INSERT INTO cron.job (schedule, command)
VALUES ('0 */12 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY gnosis_protocol_v2.view_solvers')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;