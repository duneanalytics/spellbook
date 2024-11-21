{% macro enrich_referral_rewards(model)%}

select
    r.blockchain
    ,r.project
    ,r.version
    ,r.block_number
    ,r.block_time
    ,r.block_date
    ,r.block_month
    ,r.tx_hash
    ,r.category
    ,r.referrer_address
    ,r.referee_address
    ,r.currency_contract
    ,r.reward_amount_raw
    ,r.project_contract_address
    ,r.sub_tx_id
    ,(r.referrer_address != 0x0000000000000000000000000000000000000000
      and r.reward_amount_raw > uint256 '0'
      and r.referrer_address != r.referee_address
      ) as is_referral
    ,r.tx_from
    ,r.tx_to
    ,r.reward_amount_raw/pow(10,coalesce(erc.decimals,18)) as reward_amount
    ,r.reward_amount_raw/pow(10,coalesce(erc.decimals,18))*p.price as reward_amount_usd
from {{model}} r
left join {{source('tokens', 'erc20')}} erc
    on erc.blockchain = r.blockchain
    and erc.contract_address = r.currency_contract
left join {{source('prices','usd')}} p
    on p.minute = date_trunc('minute',r.block_time)
    and (
        (p.blockchain = r.blockchain
            and p.contract_address = r.currency_contract)
        or (r.currency_contract = {{var("ETH_ERC20_ADDRESS")}}
            and p.symbol = 'ETH' and p.blockchain is null)
        )
    {% if is_incremental() %}
    and {{incremental_predicate('p.minute')}}
    {% endif %}
{% if is_incremental() %}
where {{incremental_predicate('r.block_time')}}
{% endif %}
{% endmacro %}
