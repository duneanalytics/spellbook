{{ config(
        schema = 'cow_protocol_plasma',
        alias='solvers',
        post_hook='{{ expose_spells(blockchains = \'["plasma"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["harisang"]\') }}'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_plasma', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_plasma', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
                (0x20f09d1Ded772bba7397d568436AA61c2eD434Fa, 'barn', 'Baseline'),
                (0xB3D7F3FF96768cDacDc1210A56C69CBAEE2B207a, 'prod', 'Baseline'),
                (0xC50c562c738B7773eBF46ab548df39b927b55233, 'barn', 'OKX'),
                (0xfec780B6D46EeBeFFFBCf91E5Bd7228D2E289040, 'prod', 'OKX'),
                (0xc53A02255353d76Cab69B954Ff98483a58ED3159, 'barn', 'Gnosis_BalancerSOR'),
                (0x7DC32b11E9E3444ce0fdcC60B3042375E41e15d7, 'prod', 'Gnosis_BalancerSOR'),
                (0x93cF5BF9F3FEB7F382b9f1EfFc6afdC5976CdDb9, 'barn', 'Helixbox'),
                (0x66ed980bb4E0d3a896a60a46396b636BE80657B9, 'prod', 'Helixbox'),
                (0xA3Df1497044486E09d4EB10892a07825A47427b4, 'barn', 'TSolver'),
                (0x14d6a82B819D3341D8024936a2E8B985A80B2c64, 'prod', 'TSolver'),
                (0xBC2204A90b0320183232F9D1379fEB28A9e54c60, 'barn', 'Wraxyn'),
                (0x2Cbf4401D37489C081C9E58c2d348BA5bE225135, 'prod', 'Wraxyn')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
