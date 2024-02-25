 {{
    config(
        schema='boost_ethereum',
    )
}}

{% set network_to_chain_id = {
    '1': 'ethereum',
    '10': 'optimism',
    '137': 'polygon',
    '324': 'zksync',
    '5000': 'mantle',
    '8453': 'base',
    '42161': 'arbitrum',
    '7777777': 'zora'
} %}


select
    'ethereum' as reward_network,
    contractAddress as boost_address,
    questId as boost_id,
    '' as boost_name,
    '' as action_type,
    '' as action_network,
    '' as project_name,
    questType as boost_type,
    startTime as start_time,
    endTime as end_time,
    rewardAmountOrTokenId as reward_amount_raw,
    rewardToken as reward_token_address,
    totalParticipants as max_participants,
    evt_block_time as creation_time,
    creator
from {{source('boost_ethereum', 'QuestFactory_evt_QuestCreated')}}
