 select
      'optimism' as blockchain,
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
    from {{source('rabbithole_optimism', 'QuestFactory_evt_QuestCreated')}}
