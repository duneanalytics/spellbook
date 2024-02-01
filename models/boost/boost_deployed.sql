{{
    config(
        unique_key='boost_address'
    )
}}

select
  blockchain,
  contractAddress as boost_address,
  questId as boost_id,
  contractType as boost_type,
  TRY(from_unixtime(startTime)) as start_time,
  TRY(from_unixtime(endTime)) as end_time,
  rewardAmountOrTokenId as reward_amount_or_token_id,
  rewardTokenAddress as reward_address,
  cast(totalParticipants as int) as max_participants,
  evt_block_time as creation_time,
  creator as creator_address
from
  (
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
    from
      {{source('boost_ethereum_deployed', 'rabbithole_ethereum.QuestFactory_evt_QuestCreated')}}
    union all
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
    from
      from {{source('boost_optimism_deployed', 'rabbithole_optimism.QuestFactory_evt_QuestCreated')}}
    union all
    select
      'polygon' as blockchain,
      contractAddress,
      questId,
      questType as contractType,
      startTime,
      endTime,
      rewardAmountOrTokenId,
      rewardToken,
      totalParticipants,
      evt_block_time,
      creator
    from
      {{source('boost_polygon_deployed', 'rabbithole_polygon.QuestFactory_evt_QuestCreated')}}
    union all
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
    from
      {{source('boost_arbitrum_deployed', 'rabbithole_arbitrum.QuestFactory_evt_QuestCreated')}}
    union all
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
    from
      {{source('boost_base_deployed', 'rabbithole_base.QuestFactory_evt_QuestCreated')}}
)
where 
-- from_unixtime(cast(endTime as decimal)) >= current_timestamp
creator <> 0xa4c8bb4658bc44bac430699c8b7b13dab28e0f4e -- test address
and startTime > 0
and endTime < 1e11