{%- macro balances_enrich(balances_base, blockchain, daily=false) %}
select
    {% if daily %}balances.day,{% endif %}
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
    erc20_tokens.symbol,
    erc20_tokens.decimals,
    token_id,
    nft_tokens.name as collection_name
from {{ balances_base }}
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
