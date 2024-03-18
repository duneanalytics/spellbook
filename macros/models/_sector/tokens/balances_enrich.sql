{%- macro balances_enrich(balances_raw, daily=false) %}
select
    balances.blockchain,
    {% if daily %}balances.day,{% endif %}
    balances.block_number,
    balances.block_time,
    balances.address,
    balances.token_address,
    balances.token_standard,
    balances.balance_raw,
    CASE
        WHEN balances.token_standard = 'erc20' THEN balances.balance_raw / power(10, erc20_tokens.decimals)
        WHEN balances.token_standard = 'native' THEN balances.balance_raw / power(10, 18)
        ELSE balances.balance_raw
    END as balance,
    erc20_tokens.symbol as token_symbol,
    token_id,
    nft_tokens.name as collection_name
from {{balances_raw}} balances
left join {{ ref('tokens_nft') }} nft_tokens on (
   nft_tokens.blockchain = balances.blockchain
   AND nft_tokens.contract_address = balances.token_address
   AND balances.token_standard in ('erc721', 'erc1155')
   )
left join {{ source('tokens', 'erc20') }} erc20_tokens on
    erc20_tokens.blockchain = balances.blockchain
    AND erc20_tokens.contract_address = balances.token_address
    AND balances.token_standard = 'erc20'
{% endmacro %}
