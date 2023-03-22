{{ config(alias='batch_rewards',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- PoC Query here - https://dune.com/queries/2247554
select
    block_deadline,
    block_number, -- Null here means the settlement did not occur.
    from_hex(solver) as winning_solver,
    from_hex(tx_hash) as tx_hash,
    -- Unpacking the data
    data.winning_score,
    data.reference_score,
    data.surplus,
    data.fee,
    data.execution_cost,
    data.uncapped_payment_eth,
    data.capped_payment,
    transform(data.participating_solvers, x -> from_hex(x)) as participating_solvers
from {{ source('cowswap', 'raw_batch_rewards') }}
