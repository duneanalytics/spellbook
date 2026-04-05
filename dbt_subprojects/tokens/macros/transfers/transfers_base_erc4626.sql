{% macro transfers_base_erc4626(blockchain, transactions, erc20_transfers, erc4626_deposit, erc4626_withdraw) %}
{% set token_standard_20 = 'bep20' if blockchain == 'bnb' else 'erc20' %}
{% set default_address = '0x0000000000000000000000000000000000000000' %}

-- Only include ERC4626 events from contracts that are known ERC20 tokens.
-- This filters out proxy vaults (which delegate share minting to a separate
-- token contract) and avoids double-counting their already-tracked ERC20 transfers.
with erc4626_synthetic_raw as (
    select
        t.evt_block_date as block_date
        , t.evt_block_time as block_time
        , t.evt_block_number as block_number
        , t.evt_tx_hash as tx_hash
        , t.evt_index
        , cast(null as array<bigint>) as trace_address
        , t.contract_address
        , '{{ token_standard_20 }}' as token_standard
        , {{ default_address }} as "from"
        , t.owner as "to"
        , t.shares as amount_raw
        , 'mint' as direction
        , t.owner as wallet
        , t.shares as shares
    from {{ erc4626_deposit }} as t
    inner join {{ source('tokens', 'erc20') }} as tok
        on tok.contract_address = t.contract_address
        and tok.blockchain = '{{ blockchain }}'
    {% if is_incremental() -%}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif -%}

    union all

    select
        t.evt_block_date as block_date
        , t.evt_block_time as block_time
        , t.evt_block_number as block_number
        , t.evt_tx_hash as tx_hash
        , t.evt_index
        , cast(null as array<bigint>) as trace_address
        , t.contract_address
        , '{{ token_standard_20 }}' as token_standard
        , t.owner as "from"
        , {{ default_address }} as "to"
        , t.shares as amount_raw
        , 'burn' as direction
        , t.owner as wallet
        , t.shares as shares
    from {{ erc4626_withdraw }} as t
    inner join {{ source('tokens', 'erc20') }} as tok
        on tok.contract_address = t.contract_address
        and tok.blockchain = '{{ blockchain }}'
    {% if is_incremental() -%}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif -%}
)

, erc4626_synthetic as (
    select
        block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , contract_address
        , token_standard
        , "from"
        , "to"
        , amount_raw
        , direction
        , wallet
        , shares
        , row_number() over (
            partition by tx_hash, contract_address, direction, wallet, shares
            order by evt_index
        ) as transfer_ordinal
    from erc4626_synthetic_raw
)

, erc20_mint_burn_raw as (
    select
        t.evt_tx_hash as tx_hash
        , t.contract_address
        , case
            when t."from" = {{ default_address }} then 'mint'
            else 'burn'
        end as direction
        , case
            when t."from" = {{ default_address }} then t.to
            else t."from"
        end as wallet
        , t.value as shares
        , t.evt_index
    from {{ erc20_transfers }} as t
    where (
        t."from" = {{ default_address }}
        or t.to = {{ default_address }}
    )
    {% if is_incremental() -%}
    and {{ incremental_predicate('t.evt_block_time') }}
    {% endif -%}
)

, erc20_mint_burn as (
    select
        tx_hash
        , contract_address
        , direction
        , wallet
        , shares
        , row_number() over (
            partition by tx_hash, contract_address, direction, wallet, shares
            order by evt_index
        ) as transfer_ordinal
    from erc20_mint_burn_raw
)

, unmatched_erc4626_transfers as (
    select
        c.block_date
        , c.block_time
        , c.block_number
        , c.tx_hash
        , c.evt_index
        , c.trace_address
        , c.contract_address
        , c.token_standard
        , c."from"
        , c."to"
        , c.amount_raw
    from erc4626_synthetic as c
    left join erc20_mint_burn as e
        on e.tx_hash = c.tx_hash
        and e.contract_address = c.contract_address
        and e.direction = c.direction
        and e.wallet = c.wallet
        and e.shares = c.shares
        and e.transfer_ordinal = c.transfer_ordinal
    where e.tx_hash is null
)

select
    {{ dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(t.trace_address, ',')"]) }} as unique_key
    , '{{ blockchain }}' as blockchain
    , cast(date_trunc('month', t.block_date) as date) as block_month
    , t.block_date
    , t.block_time
    , t.block_number
    , t.tx_hash
    , t.evt_index
    , t.trace_address
    , t.token_standard
    , tx."from" as tx_from
    , tx."to" as tx_to
    , tx."index" as tx_index
    , t."from"
    , t.to
    , t.contract_address
    , t.amount_raw
    , current_timestamp as _updated_at
from unmatched_erc4626_transfers as t
inner join {{ transactions }} as tx
    on
    {% if blockchain == 'gnosis' -%}
    cast(date_trunc('day', tx.block_time) as date) = t.block_date
    {% else -%}
    tx.block_date = t.block_date
    {% endif -%}
    and tx.block_number = t.block_number
    and tx.hash = t.tx_hash
    {% if is_incremental() -%}
    and {{ incremental_predicate('tx.block_time') }}
    {% endif -%}
{% endmacro -%}
