{{ config(
        schema = 'cow_protocol_avalanche_c',
        alias='solvers',
        post_hook='{{ expose_spells(blockchains = \'["avalanche_c"]\',
                                    spell_type = "project",
                                    spell_name = "cow_protocol",
                                    contributors = \'["felix"]\') }}'
)}}

WITH
-- Aggregate the solver added and removed events into a single table
-- with true/false for adds/removes respectively
solver_activation_events as (
    select solver, evt_block_number, evt_index, True as activated
    from {{ source('gnosis_protocol_v2_avalanche_c', 'GPv2AllowListAuthentication_evt_SolverAdded') }}
    union
    select solver, evt_block_number, evt_index, False as activated
    from {{ source('gnosis_protocol_v2_avalanche_c', 'GPv2AllowListAuthentication_evt_SolverRemoved') }}
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
                (0x6FD3438dfbf793B93BaecaB27d40bBB57887874A, 'barn', 'Baseline'),
                (0x742ba49f37eA8a9036A6E3CAba5ECE8DF8D988fB, 'barn', 'Gnosis_ParaSwap'),
                (0x7145E1A6711Bc974A31e704245D7030D6A5f4e26, 'barn', 'Gnosis_0x'),
                (0x632317eAFa2887C121bd0e38EA670DFd41e1D392, 'barn', 'Gnosis_BalancerSOR'),
                (0x28e4bC0656a862284854CcDB6c20765e65C664e8, 'barn', 'GlueX_Protocol'),
                (0xf2e551155E8c2cBE39d67b9C2282f41474c2F567, 'barn', 'Helixbox'),
                (0x8Bb9D4fB663A0352bf3108867aD2214F94c25bAc, 'barn', 'Tsolver'),
                (0x48fEc3a113D1647D622c2A43dd4e10Ba761d369a, 'barn', 'Unizen'),
                (0xD8b92e7d379D1dfb388B386a4bf68D549AE77EC8, 'barn', 'Wraxyn'),
                (0x667DbAD6525488c01D5C12Ff856ef03E5af75C2a, 'barn', 'ApeOut_1inch'),
                (0xa8e64684149eC4F3253904c1cD3C5a3aDb9793E5, 'barn', 'ExtQuasimodo'),
                (0x110157Cf445Afe54bb1ca284b26Ef4670e4F084B, 'barn', 'Apollo'),
                (0xcAc5830C2c62a7B2D064Ac28A8b81a78601190B4, 'barn', 'Sector'),
                (0x6c273074c5DE2711CeB7a87Fd525b00a15C65318, 'prod', 'Baseline'),
                (0xAdC54eD848Fa8e0aD9C6668D5539ebA373CA243F, 'prod', 'Gnosis_ParaSwap'),
                (0xCa674076e0f96d18347869D9B8f8f57fC098A8c5, 'prod', 'Gnosis_0x'),
                (0x6826964Ff2eD72EA0c29864796C6A72f185e5B11, 'prod', 'Gnosis_BalancerSOR'),
                (0xFC0b6360b5d24C7A0C153108698b8736c8FfaF1A, 'prod', 'GlueX_Protocol'),
                (0xad3CE441A497BC64f77b5E05Fb598F2CAfFae30e, 'prod', 'Helixbox'),
                (0x538194A8AB0a92cC60815C3bc651c34751cB4A09, 'prod', 'Tsolver'),
                (0xd58c5a371F32F0d81B48cE1bE9dBf801181F5c01, 'prod', 'Unizen'),
                (0x8cD4bEaa599AB64dD7fA2Fa8734BB0D6EC4e0af7, 'prod', 'Wraxyn'),
                (0x6390F5D0Aa64A41C622EE38D462274275c7fDE61, 'prod', 'ApeOut_1inch'),
                (0xc3D3151324F04B0d072A74AbFDa84e1A99B19b22, 'prod', 'ExtQuasimodo'),
                (0x8361B0b967b654C57a9584f15E1F6c289Fb0C13B, 'prod', 'Apollo'),
                (0xc14820F96Ac38e8f376eb9042DDa927AdB12eE02, 'prod', 'Sector')
    ) as _
)
-- Combining the metadata with current activation status for final table
select solver as address,
       case when environment is not null then environment else 'new' end as environment,
       case when name is not null then name else 'Uncatalogued'      end as name,
      active
from registered_solvers
left outer join known_solver_metadata on solver = address
