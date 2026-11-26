{{ config(
        schema = 'cow_protocol_robinhood',
        alias = 'solvers',
        materialized = 'view'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_robinhood', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_robinhood', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
                (0xE42191D47741509299861259fAb09dB5ff9276A7, 'barn', 'Baseline'),
                (0xBD7A1f33a8c58aA786f1A5598c21be55CD0294eF, 'prod', 'Baseline'),
                (0xd4536296C5B9954E391272B2bEa02bAa6Fd7BdE2, 'barn', 'Bitget Wallet'),
                (0x8D1e7461cd932cFbf2a1ce008d7d207773c61709, 'prod', 'Bitget Wallet'),
                (0x45768a148a7ad15cf6860c5e20587ffc46335425, 'barn', 'Wraxyn'),
                (0xbab6088381116915b1e1a7d715fa465f264ae0ab, 'prod', 'Wraxyn'),
                (0x5bb8a839b2b98f8fc1bb8dc1dd07b634ba530cb5, 'barn', 'ASAO'),
                (0xd56a82dbe7d406362211ad9c8da828f5165dfb22, 'prod', 'ASAO'),
                (0x8aefe99cea1aab07adfe07d36552d58110e6cf42, 'barn', 'Arc'),
                (0x8c98d45de7a520c3ecfb224ea9f0082ed7522ee8, 'prod', 'Arc'),
                (0x088375f2b5351d32c17d05a8405b8fae076796d2, 'barn', 'Helixbox'),
                (0x942ac048a7da80a76af964e3de71132e2ac031d2, 'prod', 'Helixbox'),
                (0x003fc96890bc4d9c47d12061d8ec5331e9512de4, 'barn', 'Horadrim'),
                (0xd6f435be4c6e1fc1063694f37bcccf91c2e63d2e, 'prod', 'Horadrim'),
                (0x875401ca281146ddecc30bbec2e4a14e63cc2297, 'barn', 'Zurui'),
                (0xbb22516d92c9cadc297fd140ebc3a1533b5d5b7a, 'prod', 'Zurui'),
                (0x90e34bab695cc4711ee59ca2c321af7d818d6df1, 'barn', 'Rosato'),
                (0x643b74a0a6673cb890464eaa96374d4e7de7499a, 'prod', 'Rosato'),
                (0xe7fcdb8a0fef778e44f8b10ffd9c61f2a5d1aec0, 'barn', 'Paradox'),
                (0x76d0b64e26c19ea29ed9be7c0c410936859c9961, 'prod', 'Paradox'),
                (0xfad682c64452ca6d910ad8821e07e7c8a9d551ea, 'barn', 'Nexroute'),
                (0x71c6229bf17211df514fdaf39961e27dd7715779, 'prod', 'Nexroute')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
