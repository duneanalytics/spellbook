{{ config(
        schema = 'cow_protocol_base',
        alias='solvers',
        post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["felix"]\') }}'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_base', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_base', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
                (0x8d98057b8c3d6c7cB02f1C1BE7E37D416F2D3e96, 'barn', 'Baseline'),
                (0x351DfD19DfA0e3BfD3E7D9B22658C09e66Fd14AD, 'barn', 'Seasolver'),
                (0xcE55eD17ddD7ED6E7eC396eaC772959A6D7252EA, 'barn', 'Naive'),
                (0x8982b03D56de1F6f173d04A940B20A69F6A59239, 'barn', 'Gnosis_1inch'),
                (0x5951400dE8fA58DA26Ab9402D2603ec0bD788273, 'barn', 'Gnosis_ParaSwap'),
                (0x147d05987f3008A6C9Ec3E93A4ead430907ac3E1, 'barn', 'Gnosis_0x'),
                (0x9451D27C993f7a61096BFC33e0241644a7566F66, 'barn', 'Gnosis_BalancerSOR'),
                (0x0AC9287C83C2386A6a0bb27F847Ce59a0034183C, 'barn', 'Laita'),
                (0x172FaCC5d970df43462b0f3aDe670d8cB86DC816, 'barn', 'ApeOut_1inch'),
                (0xBB765c920f86e2A2654c4B82deB5BC2E092fF93b, 'barn', 'Portus'),
                (0xDF580073E21fFd7968F317B5359B934Eb6d58804, 'barn', 'Barter'),
                (0x9775be2Bb0B72d4eA98Bfd38024EF733dc048a30, 'barn', 'Apollo'),
                (0xe0b25FA3EA727Dc34708D026ba122625B98A94FB, 'barn', 'GlueX_Protocol'),
                (0x707Dfa95835542A6528fD077c351446f497276CF, 'barn', 'Rizzolver'),
                (0x4eAD087d78C21Fd95D30411928A2Ade7456f56F4, 'barn', 'OKX'),
                (0x07caD32e40A92a86E7F2E7b373BAaf4704d92c5b, 'barn', 'Elfomo'),
                (0x2C975c34D54AD06607f8ea14519c36f91275349d, 'prod', 'Elfomo'),
                (0xd875cd50B179a046512C80edF6CB2C1Fc3F3072D, 'prod', 'OKX'),
                (0x914db7338ACAe3f3866B79DcEfFcFBCC554F18ed, 'prod', 'Rizzolver'),
                (0xc1b0bB599c578a846b51EE5dcE3d9FAD69528613, 'prod', 'GlueX_Protocol'),
                (0x41f387db8470c99b7f376212075e2E289f085Ce9, 'prod', 'Apollo'),
                (0x36Fd8A0C24B08F7bb4af8d6eaA6245C3884fC682, 'prod', 'Barter'),
                (0x1a72876ebE781E42aB2Ee4278B539688D8B80E2D, 'prod', 'ApeOut_1inch'),
                (0x6bf97aFe2D2C790999cDEd2a8523009eB8a0823f, 'prod', 'Portus'),
                (0x69d7F96dFD091652f317D0734A5F2B492ACcbE07, 'prod', 'Baseline'),
                (0x4cb862E4821fea2dabBD1f0A69c17d52da2A58f6, 'prod', 'Seasolver'),
                (0xF401ceF222F1CA2fE84a8C7BFC75A636A4542A74, 'prod', 'Naive'),
                (0x8F7f754300B1ccfa37eA25fD48FB059af0F19e12, 'prod', 'Gnosis_1inch'),
                (0xe321609c56aD89711EfB69c248ebe94922902F81, 'prod', 'Gnosis_ParaSwap'),
                (0xbBcCE072fb1Bd2C096667E257322f47693D3dc96, 'prod', 'Gnosis_0x'),
                (0x983aC485620E265730e367B2C7BCBf6Eb9d62A21, 'prod', 'Gnosis_BalancerSOR'),
                (0x1A422923290fd16C2ED00ED16B4203cF4bb35d82, 'prod', 'Laita'),
                (0x09E5CdfEEaC1866103E17e1debf4aad61c1904eF, 'prod', 'Sector_Finance')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
