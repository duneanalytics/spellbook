 {{
    config(
        schema='boost_ethereum',
    )
}}
select
    'ethereum' as blockchain,
    contractAddress,
    questId,
    questType as contractType,
    startTime,
    endTime,
    rewardAmountOrTokenId,
    rewardToken as rewardTokenAddress,
    totalParticipants,
    evt_block_time,
    creator
from {{source('boost_ethereum', 'QuestFactory_evt_QuestCreated')}}
