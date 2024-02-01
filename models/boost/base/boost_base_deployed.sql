 select
      'base' as blockchain,
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
    from {{source('boost_base_deployed', 'rabbithole_base.QuestFactory_evt_QuestCreated')}}