 {{
    config(
        schema='boost_optimism',
        alias='deployed',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['boost_address', 'boost_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_time')]
    )
}}

{% set drakula_network_bug_fix_timestamp = '2024-06-29' %}

{% set network_to_chain_id = {
    '1': 'ethereum',
    '10': 'optimism',
    '137': 'polygon',
    '324': 'zksync',
    '5000': 'mantle',
    '8453': 'base',
    '42161': 'arbitrum',
    '81457': 'blast',
    '7777777': 'zora'
} %}

select
    'optimism' as reward_network,
    contractAddress as boost_address,
    questId as boost_id,
    questName as boost_name,
    actionType as action_type,
    case
        when creator = 0xe627b03b7fe363e840dab2debf8b962c672e89fb 
            and evt_block_time <= timestamp '{{ drakula_network_bug_fix_timestamp }}'
        then 'base' -- fix Drakula wrong action network bug 
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
from {{source('boost_optimism', 'QuestFactory_evt_QuestCreated')}}
where questId <> 'd070f682-e513-4585-9dc8-e973c8ff6a7c'
{% if is_incremental() %}
and
    {{ incremental_predicate('evt_block_time') }}
{% endif %}
