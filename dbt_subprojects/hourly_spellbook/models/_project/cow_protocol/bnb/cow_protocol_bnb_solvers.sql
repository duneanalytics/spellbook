{{ config(
        schema = 'cow_protocol_bnb',
        alias='solvers',
        post_hook='{{ expose_spells(blockchains = \'["bnb"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["harisang"]\') }}'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_bnb', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_bnb', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
                (0xe0eDF50543eb5608015Fd9F08e399569EFa4d08c, 'barn', 'Baseline'),
                (0x98301405Fdc87Db9db736624C5eC37705318CAD5, 'prod', 'Baseline'),
                (0x333C4eB595e535FD87D44829d8a6D856C668b612, 'barn', 'Gnosis_0x'),
                (0xC03A8652d4807493824F174C49fc87c92AB03f88, 'prod', 'Gnosis_0x'),
                (0x2A3Eb1c572f7B20a2bA3EcEc77955d15e9bCDf95, 'barn', 'Gnosis_BalancerSOR'),
                (0x0Fe94721e4936b144ab9f220805290f2fF9a9434, 'prod', 'Gnosis_BalancerSOR'),
                (0x5e4Acd1E760e2DbF82E64deFbfE3ceeD8a38cdDe, 'barn', 'Unizen'),
                (0xb272C6580244F5716075C2833C0fAFe7178c22DC, 'prod', 'Unizen'),
                (0x9D990Acb053364d5821EeEb03043712AC0cD0439, 'barn', 'Helixbox'),
                (0x5920Fe8e1E7F0014f217d0ed3920A403659f73cd, 'prod', 'Helixbox'),
                (0x397F0423D59fBf9Bfc2D8f3985D3e94923318200, 'barn', 'Kipseli'),
                (0x6f9a7AFe6446ABfa91f0954c3cd1677B87247FEf, 'prod', 'Kipseli'),
                (0xCc5fb0080B73070994bF0438469bC708FE2ac21a, 'barn', 'ExtQuasimodo'),
                (0xC10484e51Ef5e69Dd0aC473A1E5310720391A174, 'prod', 'ExtQuasimodo'),
                (0x4dd1be0Cd607E5382Dd2844fA61D3a17e3e83D56, 'prod', 'Rizzolver'),
                (0xF49DC3F9Fd153CF8d11d38E79BEdC28C3d62bBb7, 'barn', 'OpenOcean_Aggregator'),
                (0x784260C2664536AE5d4d92cAD886B00a532eA84C, 'prod', 'OpenOcean_Aggregator'),
                (0x18E302738f1eAec0CA96894a949b2bCCD02CDd8C, 'barn', 'Wraxyn'),
                (0x943C94CD0374cb2Ba3C88Bab1bDC8393745c5fE9, 'prod', 'Wraxyn'),
                (0x347120a515D640dC9ee88Ab060ae8f4f482d3D7A, 'barn', 'MXTrading'),
                (0x8979cffddb57b0bb9f507bd99b3f98bc66e70197, 'prod', 'MXTrading'),
                (0x3980daa7eaad0b7e0c53cfc5c2760037270da54d, 'prod', 'Tsolver'),
                (0xBB765c920f86e2A2654c4B82deB5BC2E092fF93b, 'barn', 'Portus')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
