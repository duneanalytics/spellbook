{%- macro balances_enrich(balances_base) %}
select
    balances.blockchain,
    balances.block_number,
    balances.block_time,
    balances.tx_hash,
    balances.tx_index,
    balances.evt_index,
    balances.transfer_type,
    balances.wallet_address,
    balances.token_address,
    balances.change_amount_raw,
    balances.change_amount_raw / power(10, erc20_tokens.decimals) as change_amount,
    balances.change_amount_raw / power(10, prices.decimals) * prices.price as change_amount_usd,
    balance_raw,
    balances.balance_raw / power(10, erc20_tokens.decimals) as balance,
    (balances.balance_raw / power(10, prices.decimals) * prices.price)  as balance_usd,
    prices.price as price_rate
from {{ balances_base }} balances
left join {{ ref('tokens_erc20') }}  erc20_tokens on
    erc20_tokens.blockchain = balances.blockchain
    erc20_tokens.contract_address = balances.token_address
left join {{ source('prices', 'usd') }} prices on
    prices.blockchain = balances.blockchain
    and prices.contract_address = balances.token_address
    and prices.minute = date_trunc('minute', balances.evt_block_time)
{% endmacro %}
