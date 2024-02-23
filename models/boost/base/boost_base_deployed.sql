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
from {{source('boost_base', 'QuestFactory_evt_QuestCreated')}}
