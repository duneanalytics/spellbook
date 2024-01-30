{%- macro balances_enrich(balances_base, blockchain) %}
select
    balances.block_number,
    balances.block_time,
    balances.type,
    balances.address as wallet_address,
    balances.contract_address as token_address,
    balances.amount as balance_raw,
    CASE
        WHEN balances.type = 'erc20' THEN balances.amount / power(10, erc20_tokens.decimals)
        WHEN balances.type = 'native' THEN balances.amount / power(10, 18)
        ELSE balances.amount
    END as balance,
    CASE
        WHEN balances.type = 'erc20' THEN balances.amount / power(10, erc20_tokens.decimals) * prices.price
        WHEN balances.type = 'native' THEN balances.amount / power(10, 18) * prices.price
        ELSE NULL
    END as balance_usd,
    prices.price as price_rate,
    erc20_tokens.symbol,
    erc20_tokens.decimals,
    token_id,
    nft_tokens.name as collection_name
from   {{ source('prices', 'usd') }} prices
right join {{ balances_base }} balances on (
    CASE
        WHEN type = 'erc20' THEN prices.contract_address = balances.contract_address and prices.blockchain = '{{ blockchain }}'
        WHEN type = 'native' THEN prices.contract_address is null and prices.symbol = 'ETH' and prices.blockchain is null
        ELSE false
    END)
    and prices.minute = date_trunc('minute', balances.block_time)
left join {{ source('tokens', 'erc20') }} erc20_tokens on
    erc20_tokens.blockchain = '{{ blockchain }}' AND (
    CASE
        WHEN type = 'erc20' THEN erc20_tokens.contract_address = balances.contract_address
        -- TODO: should not be hardcoded
        WHEN type = 'native' THEN erc20_tokens.contract_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        ELSE false
    END)
left join {{ ref('tokens_nft') }} nft_tokens on (
   nft_tokens.blockchain = '{{ blockchain }}' AND (
   CASE
        WHEN (type = 'erc721' OR type = 'erc1155') THEN nft_tokens.contract_address = balances.contract_address
        ELSE false
    END
    )
)
{% endmacro %}
