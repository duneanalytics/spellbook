{{ config(
        schema = 'cow_protocol_ink',
        alias='solvers'
        , post_hook='{{ hide_spells() }}'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_ink', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_ink', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
    select *
    from (
      VALUES
                (0x5004BD66C0804C7794e826B6DeE9Cc83754224E8, 'barn', 'Baseline'),
                (0x8b2202E91Afa5ad33DC04Caabd34191b6400E6a6, 'prod', 'Baseline'),
                (0x7Fab5B9D65ae105Cf524BCF8F37cDeAaE1F37096, 'barn', '0x API'),
                (0xC290cDE37b5d35FDC92BFd71310ce6327666F97f, 'prod', '0x API'),
                (0x29625eC643F13dA0b20Fff8BB757fCc3768a55fc, 'barn', 'Helixbox'),
                (0x539A8cAFD0698dDD2E833121E3dff977dBE76bF6, 'prod', 'Helixbox'),
                (0xB3b5041C8200d2dA875610189437213CE7f6f759, 'barn', 'Tsolver'),
                (0x22478dCFb16f496Fd3C11080C95818DcEbA3dC43, 'prod', 'Tsolver'),
                (0xDBb5E06bB7353Ef0499d875fdD5C779d4f4358dA, 'barn', 'Paradox'),
                (0xB0e2D501Fc499A4b4E2540adE38fEA2A29Cc157e, 'prod', 'Paradox'),
                (0xc89a922687B120210b7E8b0f448AB3F90Ae123E5, 'barn', 'Rosato'),
                (0x9458ab3878E7f120e548FA65EBcC7fF61E771E0C, 'prod', 'Rosato'),
                (0x40D743DE406898A94fD64a6A0AFd833371f721F2, 'barn', 'ExtQuasimodo'),
                (0xda7e3EF012AC3F2DeC899269950947061c2B0134, 'prod', 'ExtQuasimodo')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
