 {{
    config(
        schema='boost_base',
    )
}}
select
      'base' as blockchain,
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
from {{source('rabbithole_base', 'QuestFactory_evt_QuestCreated')}}
