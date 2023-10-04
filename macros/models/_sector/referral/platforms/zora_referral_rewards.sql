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
    ,evt_tx_hash as tx_hash
    ,'NFT' as category
    ,mintReferral as referrer_address
    ,cast(null as varbinary) as referee_address     -- will be overwritten as tx_from
    ,{{ var("ETH_ERC20_ADDRESS") }} as currency_contract
    ,mintReferralReward as reward_amount_raw
    ,"from" as project_contract_address     -- the drop contract
    ,evt_index as sub_tx_id
from {{ProtocolRewards_evt_RewardsDeposit}}
{% if is_incremental %}
WHERe evt_block_time > date_trunc('day', now() - interval '1' day)
{% endif %}
{% endmacro %}
