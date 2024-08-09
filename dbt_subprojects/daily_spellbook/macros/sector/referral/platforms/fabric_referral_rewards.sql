{% macro fabric_referral_rewards(
    blockchain
    ,SubscriptionTokenV1_evt_ReferralPayout
    ,SubscriptionTokenV1Factory_call_deploySubscription
    )
%}

with currency_contract_info as (
    select
        output_0,
        erc20TokenAddr
    from {{SubscriptionTokenV1Factory_call_deploySubscription}}
)

select
    '{{blockchain}}' as blockchain
    ,'fabric' as project
    ,'v1' as version
    ,evt_block_number as block_number
    ,evt_block_time as block_time
    ,cast(date_trunc('day',evt_block_time) as date) as block_date
    ,cast(date_trunc('month',evt_block_time) as date) as block_month
    ,evt_tx_hash as tx_hash
    ,'NFT' as category
    ,referrer as referrer_address
    ,tx."from" as referee_address
    ,c.erc20TokenAddr as currency_contract
    ,"rewardAmount" as reward_amount_raw
    ,e."contract_address" as project_contract_address
    ,evt_index as sub_tx_id
    ,tx."from" as tx_from
    ,tx.to as tx_to
from {{SubscriptionTokenV1_evt_ReferralPayout}} e
inner join {{source(blockchain, 'transactions')}} tx
    on e.evt_block_number = tx.block_number
    and e.evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and {{incremental_predicate('tx.block_time')}}
    {% endif %}
left join currency_contract_info c
    on e.contract_address = c.output_0
{% if is_incremental() %}
where {{incremental_predicate('evt_block_time')}}
{% endif %}
{% endmacro %}
