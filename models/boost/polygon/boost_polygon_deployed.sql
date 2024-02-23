 {{
    config(
        schema='boost_polygon',
    )
}}
select
      'polygon' as blockchain,
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
from {{source('boost_polygon', 'QuestFactory_evt_QuestCreated')}}
