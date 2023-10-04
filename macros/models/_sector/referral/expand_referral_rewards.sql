{% macro expand_referral_rewards(
    blockchain
    ,rewards_cte)
    %}



select
    r.blockchain
    ,r.project
    ,r.version
    ,r.block_number
    ,r.block_time
    ,r.tx_hash
    ,r.category
    ,r.referrer_address
    ,coalesce(r.referee_address, t."from") as referee_address
    ,r.currency_contract
    ,r.reward_amount_raw
    ,r.project_contract_address
    ,r.sub_tx_id

    ,t."from" as tx_from
    ,t.to as tx_to

    ,r.reward_amount_raw/pow(10,coalesce(erc.decimals,18)) as reward_amount
    ,r.reward_amount_raw/pow(10,coalesce(erc.decimals,18))*p.price as reward_amount_usd,
from {{rewards_cte}} r
left join {{source(blockchain,'transactions')}} t
    on r.block_number = t.block_number and r.tx_hash = t.hash
left join {{ref('tokens_erc20')}} erc
    on erc.blockchain = '{{blockchain}}'
    and erc.contract_address = r.currency_contract
left join {{ref('prices_usd_forward_fill')}} p
    on (p.blockchain = '{{blockchain}}'
        and p.contract_address = r.currency_contract)
    or (r.currency_contract = {{var("ETH_ERC20_ADDRESS")}}
        and p.symbol = 'ETH' and p.blockchain = null
        )
{% endmacro %}
