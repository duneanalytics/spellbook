{{
    config(
        unique_key='boost_address'
    )
}}

{% set boost_deployed_models = [
    ref('boost_arbitrum_deployed'),
    ref('boost_base_deployed'),
    ref('boost_ethereum_deployed'),
    ref('boost_optimism_deployed'),
    ref('boost_polygon_deployed'),
] %}

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
    {% for model in boost_deployed_models %}
)
where 
    creator <> 0xa4c8bb4658bc44bac430699c8b7b13dab28e0f4e -- test address
    and startTime > 0
    and endTime < 1e11
