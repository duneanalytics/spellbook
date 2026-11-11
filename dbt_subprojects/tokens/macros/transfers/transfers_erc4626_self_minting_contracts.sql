{% macro transfers_erc4626_self_minting_contracts(blockchain, erc20_transfers, erc4626_deposit, erc4626_withdraw) %}
{% set default_address = '0x0000000000000000000000000000000000000000' %}

-- ERC4626 contracts that emit their own ERC20 mint/burn Transfer events.
-- transfers_base_erc4626 builds no synthetic mint/burn rows for these contracts.
-- Incremental runs scan only the run window of evt_Transfer and add contracts not seen
-- before. Unlike a full refresh, they miss a contract whose only mint/burn came before
-- its first Deposit/Withdraw event.
with erc4626_contracts as (
    select contract_address from {{ erc4626_deposit }}
    union
    select contract_address from {{ erc4626_withdraw }}
)

select
    '{{ blockchain }}' as blockchain
    , t.contract_address
    , min(t.evt_block_time) as first_mint_burn_block_time
from {{ erc20_transfers }} as t
inner join erc4626_contracts as c
    on c.contract_address = t.contract_address
where (
    t."from" = {{ default_address }}
    or t.to = {{ default_address }}
)
{% if is_incremental() -%}
and {{ incremental_predicate('t.evt_block_time') }}
and t.contract_address not in (select contract_address from {{ this }})
{% endif -%}
group by
    t.contract_address
{% endmacro %}
