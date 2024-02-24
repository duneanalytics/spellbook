{{
    config(
        unique_key='boost_address',
        schema='boost',
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
    reward_network,
    boost_address,
    boost_id,
    boost_name,
    action_type,
    action_network,
    project_name,
    questType as boost_type,
    TRY(from_unixtime(startTime)) as start_time,
    TRY(from_unixtime(endTime)) as end_time,
    reward_amount_raw,
    reward_token_address,
    cast(max_participants as int) as max_participants,
    creation_time,
    creator as creator_address
from
  (
    {% for model in boost_deployed_models %}
    SELECT *
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
where 
    creator <> 0xa4c8bb4658bc44bac430699c8b7b13dab28e0f4e
    and startTime > 0
    and endTime < 1e11
