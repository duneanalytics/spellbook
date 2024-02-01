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
    from {{source('boost_polygon_deployed', 'rabbithole_polygon.QuestFactory_evt_QuestCreated')}}