{% macro zora_referral_rewards(
    blockchain
    ,ProtocolRewards_evt_RewardsDeposit
    )
%}

select
    '{{blockchain}}' as blockchain
    ,'zora' as project
    ,'v1' as version
    ,evt_block_number as block_number
    ,evt_block_time as block_time
    ,cast(date_trunc('day',evt_block_time) as date) as block_date
    ,cast(date_trunc('month',evt_block_time) as date) as block_month
    ,evt_tx_hash as tx_hash
    ,'NFT' as category
    ,case
        when mintReferralReward = uint256 '0'
        or mintReferral = zora
        or mintReferral = tx."from"
        then 0x0000000000000000000000000000000000000000 else mintReferral end as referrer_address
    ,cast(null as varbinary) as referee_address     -- will be overwritten as tx_from
    ,{{ var("ETH_ERC20_ADDRESS") }} as currency_contract
    ,mintReferralReward as reward_amount_raw
    ,"from" as project_contract_address     -- the drop contract
    ,evt_index as sub_tx_id
    ,tx."from" as tx_from
    ,tx.to as tx_to
from {{ProtocolRewards_evt_RewardsDeposit}}
inner join {{source(blockchain, 'transactions')}} tx
    on evt_block_number = tx.block_number
    and evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and tx.block_time > date_trunc('day', now() - interval '1' day)
    {% endif %}
{% if is_incremental() %}
where evt_block_time > date_trunc('day', now() - interval '1' day)
{% endif %}
{% endmacro %}
