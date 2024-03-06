{{
    config(
        schema='boost',
        alias='claimed_legacy'
    )
}}

{% set erc20_quests = ['arbitrum', 'base', 'optimism', 'polygon'] %}
{% set receipt_quests = ['ethereum', 'optimism', 'polygon'] %}

with receipt_mints as (
    {% for network in receipt_quests %}
    {% set schema_name = 'boost_' + network %}
    {% set deployed_model = ref('boost_' + network + '_deployed') %}
    select
        '{{ network }}' as reward_network,
        c.questAddress as boost_address,
        c.questId as boost_id,
        '' as boost_name,
        c.recipient as claimer_address,
        q.reward_amount_raw,
        q.reward_token_address,
        c.evt_tx_hash as claim_tx_hash,
        c.evt_block_time as block_time
    from {{ source(schema_name, 'QuestFactory_evt_ReceiptMinted') }} c
    join {{ deployed_model }} q
    on c.questId = q.boost_id
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
),
erc20_claims as (
    {% for network in erc20_quests %}
    {% set schema_name = 'boost_' + network %}
    select
        '{{ network }}' as reward_network,
        questAddress as boost_address,
        questId as boost_id,
        '' as boost_name,
        recipient as claimer_address,
        rewardAmountInWei as reward_amount_raw,
        rewardToken as reward_token_address,
        evt_tx_hash as claim_tx_hash,
        evt_block_time as block_time
    from {{ source(schema_name, 'QuestFactory_evt_QuestClaimed') }}    
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
),
erc1155_claims as (
    {% for network in erc20_quests %}
    {% set schema_name = 'boost_' + network %}
    select
        '{{ network }}' as reward_network,
        questAddress as boost_address,
        questId as boost_id,
        '' as boost_name,
        recipient as claimer_address,
        cast(1 as uint256) as reward_amount_raw,
        rewardToken as reward_token_address,
        evt_tx_hash as claim_tx_hash,
        evt_block_time as block_time
    from {{ source(schema_name, 'QuestFactory_evt_Quest1155Claimed') }}    
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
),
unified_claims_legacy as (
    select
        *, if(block_time >= date '2023-09-13', 0.000075, 0.00005) as claim_fee_eth
    from erc20_claims
    where block_time <= date '2023-11-12'

    union all 
    select
        *, if(block_time >= date '2023-09-13', 0.000075, 0.00005) as claim_fee_eth
    from erc1155_claims
    where block_time <= date '2023-11-12'

    union all
    select
        *, 0 as claim_fee_eth
    from receipt_mints
    where block_time <= date '2023-07-15'
)

select distinct
    reward_network, 
    boost_address,
    boost_id,
    boost_name,
    '' as project_name,
    claimer_address,
    reward_amount_raw,
    reward_token_address,
    claim_tx_hash,
    block_time,
    claim_fee_eth,
    '' as action_type,
    cast(NULL as varbinary) action_tx_hash,
    '' as action_network
from unified_claims_legacy
