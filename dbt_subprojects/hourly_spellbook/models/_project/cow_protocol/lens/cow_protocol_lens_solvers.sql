{{ config(
        schema = 'cow_protocol_lens',
        alias='solvers',
        post_hook='{{ expose_spells(blockchains = \'["lens"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["harisang"]\') }}'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_lens', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_lens', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
                (0xA72303608C0Ce45df0B1b4CaBA1f5E854f4023e7, 'barn', 'Baseline'),
                (0x70b85fD4FD23D8746ecF6221FeF57da2AC5031f7, 'barn', 'ExtQuasimodo'),
                (0x1939B77693c4c477B4c6b75965Bef05356A756EF, 'barn', 'Helixbox'),
                (0x31be7a14067bA4f3549b51cAAED8358f185faF90, 'prod', 'Baseline'),
                (0x853eA89E44BEaf1dF628A6ECC71231cADe769553, 'prod', 'ExtQuasimodo'),
                (0x2843af285B7e82f2CBc96A45289884F55a460EDe, 'prod', 'Helixbox')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
