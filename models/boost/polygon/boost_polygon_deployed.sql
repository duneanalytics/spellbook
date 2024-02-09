 {{
    config(
        schema='boost_polygon',
    )
}}
select
      'polygon' as blockchain,
      contractAddress,
      questId,
      contractType,
      startTime,
      endTime,
      rewardAmountOrTokenId,
      rewardTokenAddress,
      totalParticipants,
      evt_block_time,
      creator
from {{source('rabbithole_polygon', 'QuestFactory_evt_QuestCreated')}}
