{% test oneinch_no_cross_chain_placeholder_tokens(model) %}

{% set placeholder_tokens = oneinch_cross_chain_placeholder_tokens_cfg_macro() | join(', ') %}

select tx_hash, token_bought_address, token_sold_address
from {{ model }}
where token_bought_address in ({{ placeholder_tokens }})
   or token_sold_address in ({{ placeholder_tokens }})

{% endtest %}
