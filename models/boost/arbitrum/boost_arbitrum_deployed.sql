 {{
    config(
        schema='boost_arbitrum',
        alias='deployed',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['boost_address', 'boost_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_time')]
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
    'arbitrum' as reward_network,
    contractAddress as boost_address,
    questId as boost_id,
    questName as boost_name,
    actionType as action_type,
    case
    {% for chain_id, network in network_to_chain_id.items() %}
        when chainId={{ chain_id }} then '{{ network }}'
    {% if loop.last %}
    end as action_network,
    {% endif %}
    {% endfor %}
    projectName as project_name,
    coalesce(contractType, questType) as boost_type,
    startTime as start_time,
    endTime as end_time,
    rewardAmountOrTokenId as reward_amount_raw,
    coalesce(rewardTokenAddress, rewardToken) as reward_token_address,
    totalParticipants as max_participants,
    evt_block_time as creation_time,
    creator
from {{source('boost_arbitrum', 'QuestFactory_evt_QuestCreated')}}
{% if is_incremental() %}
where
    {{ incremental_predicate('evt_block_time') }}
{% endif %}
