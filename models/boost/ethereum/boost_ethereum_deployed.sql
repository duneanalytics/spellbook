 select
      'ethereum' as blockchain,
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
    from {{source('boost_ethereum_deployed', 'rabbithole_ethereum.QuestFactory_evt_QuestCreated')}}
