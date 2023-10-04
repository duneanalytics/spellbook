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
    ,cast(date_trunc('day',r.block_time) as date) as block_date
    ,cast(date_trunc('month',r.block_time) as date) as block_month
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
    ,r.reward_amount_raw/pow(10,coalesce(erc.decimals,18))*p.price as reward_amount_usd
from {{rewards_cte}} r
left join {{source(blockchain,'transactions')}} t
    on r.block_number = t.block_number and r.tx_hash = t.hash
    {% if is_incremental %}
    WHERE t.block_time > date_trunc('day', now() - interval '1' day)
    {% endif %}
left join {{ref('tokens_erc20')}} erc
    on erc.blockchain = '{{blockchain}}'
    and erc.contract_address = r.currency_contract
left join {{ref('prices_usd_forward_fill')}} p
    on p.minute = date_trunc('minute',r.block_time)
    and (
        (p.blockchain = '{{blockchain}}'
            and p.contract_address = r.currency_contract)
        or (r.currency_contract = {{var("ETH_ERC20_ADDRESS")}}
            and p.symbol = 'ETH' and p.blockchain = null
        )
    {% if is_incremental %}
    and p.minute > date_trunc('day', now() - interval '1' day)
    {% endif %}
{% if is_incremental %}
WHERE r.block_time > date_trunc('day', now() - interval '1' day)
{% endif %}
{% endmacro %}
