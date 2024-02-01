 select
      'arbitrum' as blockchain,
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
    from {{source('boost_arbitrum_deployed', 'rabbithole_arbitrum.QuestFactory_evt_QuestCreated')}}
    -- rabbithole_arbitrum.QuestFactory_evt_QuestCreated