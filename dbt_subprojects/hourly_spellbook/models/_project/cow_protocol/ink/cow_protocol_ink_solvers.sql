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
-- TODO: Get correct Ink solver addresses from solver team
known_solver_metadata (address, environment, name) as (
    select *
    from (
      VALUES
                (0xeeD424986A909d3552CB6D108840E16B41DfD6ac, 'barn', 'Baseline'),
                (0xD72728C165C2e941d41749026c41d414C6679636, 'prod', 'Baseline'),
                (0x57dc4958b5691D3C6A5b58c71a5074eeC7eE7888, 'barn', 'OKX'),
                (0x920E180FbB32D2B14EA51D95630afe3F8205b278, 'prod', 'OKX'),
                (0x975CF2c0897bf910F60b21693eD304374F79cD44, 'barn', 'Helixbox'),
                (0x6a18b02a1E4530886842DC1D0c41869F5819c98a, 'prod', 'Helixbox'),
                (0xBfd885cC9b21e8a167eC41577520ef133d4aF36B, 'barn', 'Wraxyn'),
                (0x2bE0F2E120938DB793764D9e9Ee123c3CF21FEdc, 'prod', 'Wraxyn'),
                (0x674325BbAdBb66e06A674Fd69f7b40fE01aB1De5, 'barn', 'Tsolver'),
                (0x13C8360b175C1eB7cbA7d11DD91aEF0a1A79ab08, 'prod', 'Tsolver')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
