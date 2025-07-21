{{ config(
        schema = 'cow_protocol_polygon',
        alias='solvers',
        post_hook='{{ expose_spells(blockchains = \'["polygon"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["felix"]\') }}'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_polygon', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_polygon', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
                (0xdD1e44997D26e465aD7378BDFc341598B5377fcf, 'barn', 'Baseline'),
                (0x2AeC209d0776F39b055C132A42d50eBed4d58Fb8, 'barn', 'Gnosis_ParaSwap'),
                (0x27042400D8C59f4d6d214072b90D3de5276c4Bc4, 'barn', 'Gnosis_0x'),
                (0x58354D109Af1a1B8934048AF22da15A5EF978BE3, 'barn', 'Gnosis_BalancerSOR'),
                (0xc30067F1a55FC1C6D5f1ff1756995C43Bb851D25, 'barn', 'GlueX_Protocol'),
                (0x695b6b6fc2733455cf0660b748A39Ba7FdD1c7AB, 'barn', 'Helixbox'),
                (0x0dE2AC3785b94A16Cd726292Ba62D9584434B880, 'barn', 'Tsolver'),
                (0xE9F0F95616080ff0f0b452c5b1f6042fAd03eFF5, 'barn', 'Unizen'),
                (0x2E1b57EE3C5085A22e48D24E1D7b7A043fa0F9e9, 'barn', 'Wraxyn'),
                (0xd772a0d831de079f1902fC9F708272ABEBf83220, 'barn', 'ApeOut_1inch'),
                (0x4766647F8fC4BFE9756BFAa40cc7879cCA853Dbc, 'barn', 'Sector'),
                (0x8687a369dcEC04077Bf97026ef01A95bA3E452a2, 'barn', 'Apollo'),
                (0xe25E4203bb3c4881214F791d0659cd4D6a6B897A, 'prod', 'Baseline'),
                (0x1cDe7808706dB1654644565a1039Fce1e5f9c168, 'prod', 'Gnosis_ParaSwap'),
                (0x4b277FB0BA7aDFF366B20470096b3c4709E990A0, 'prod', 'Gnosis_0x'),
                (0xCf7e5Ba0f73bfDa5C4BeDA66fB01179E292F7EDB, 'prod', 'Gnosis_BalancerSOR'),
                (0xA3c5eF7E9346CF1128c421511b8CfA94F19Df503, 'prod', 'GlueX_Protocol'),
                (0xC44Afc0d1b5913A3378DE5138b4F04e63713F82D, 'prod', 'Helixbox'),
                (0x1Cbf92c76b0Ed66Aa4fc38e73f9Bc60e812120fC, 'prod', 'Tsolver'),
                (0xF0717C3E480058b260359Fa642f896D61B14a9f4, 'prod', 'Unizen'),
                (0x8518181b9E31C139542bdA727Ae147d7aE67F2D4, 'prod', 'Wraxyn'),
                (0x7776c6188143C22a0805FFdF0D604146C7885027, 'prod', 'ApeOut_1inch'),
                (0x92Af001AF846450EA5B2DFb286B5be559E741300, 'prod', 'Sector'),
                (0x6715174ADe6ac9fB8E0d5461b8051F7DD496798B, 'prod', 'Apollo')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
