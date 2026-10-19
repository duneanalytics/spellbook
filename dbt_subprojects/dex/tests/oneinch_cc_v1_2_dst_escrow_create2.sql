-- Each successful EscrowFactoryV1_2.createDstEscrow call on robinhood, cronos, monad, hyperevm and arc
-- must decode all v1.2 immutables fields and compute a CREATE2 dst-escrow address that exists in
-- <chain>.creation_traces in the same transaction.

{% set chains = [
    ("robinhood", "0x50d26ea1e2460b3a42ff47466b955fc6bd906013", "2026-07-15"),
    ("cronos"   , "0x8e6c3c2e2631de0a1d4fd46a15f79a1373486fa4", "2026-08-01"),
    ("monad"    , "0x8e6c3c2e2631de0a1d4fd46a15f79a1373486fa4", "2026-08-01"),
    ("hyperevm" , "0x8e6c3c2e2631de0a1d4fd46a15f79a1373486fa4", "2026-08-01"),
    ("arc"      , "0x8e6c3c2e2631de0a1d4fd46a15f79a1373486fa4", "2026-09-01"),
] %}

with

dst_creations as (
    {% for chain, factory, start in chains %}
        select
            blockchain
            , block_date
            , tx_hash
            , call_trace_address
            , escrow
            , order_hash
            , hashlock
            , maker
            , taker
            , token
            , amount
            , safety_deposit
            , timelocks
        from {{ ref('oneinch_' + chain + '_cc') }}
        where block_date >= date '{{ start }}'
            and contract_name = 'EscrowFactoryV1_2'
            and flow = 'dst_creation'
            and call_success
            and tx_success
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
)

, creation_traces as (
    {% for chain, factory, start in chains %}
        select
            '{{ chain }}' as blockchain
            , tx_hash
            , address
        from {{ source(chain, 'creation_traces') }}
        where block_time >= timestamp '{{ start }}'
            and "from" = {{ factory }}
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
)

select
    d.blockchain
    , d.block_date
    , d.tx_hash
    , d.call_trace_address
    , d.escrow
from dst_creations as d
left join creation_traces as c
    on c.blockchain = d.blockchain
    and c.tx_hash = d.tx_hash
    and c.address = d.escrow
where c.address is null
    or d.escrow is null
    or d.order_hash is null
    or d.hashlock is null
    or d.maker is null
    or d.taker is null
    or d.token is null
    or d.amount is null
    or d.safety_deposit is null
    or d.timelocks is null
