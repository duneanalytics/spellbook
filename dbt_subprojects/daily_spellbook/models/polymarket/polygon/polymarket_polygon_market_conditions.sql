{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_conditions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['question_id','condition_id','condition_token'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with conditions as (
  select
    bmc.block_time,
    bmc.block_number,
    bmc.question_id,
    bmc.condition_id,
    ctf.token0 as condition_token,
    case when row_number() over (partition by ctf.condition_id order by ctf.evt_index) = 1 then 'yes' else 'no' end as condition_status,
    bmc.outcome_slot_count,
    bmc.oracle,
    bmc.evt_index,
    bmc.tx_hash
  from {{ ref('polymarket_polygon_base_market_conditions') }} bmc
    inner join {{ ref('polymarket_polygon_base_ctf_tokens') }} ctf on bmc.condition_id = ctf.condition_id
  {% if is_incremental() %}
  where {{ incremental_predicate('bmc.block_time') }}
  {% endif %}
),

-- credit to @cryptokoryo_research for this query as inspiration for logic below: https://dune.com/queries/3908099
markets as (
  select
    block_time,
    block_number,
    market_id,
    question_id,
    question,
    evt_index,
    tx_hash
  from {{ ref('polymarket_polygon_markets') }}
  where market_id is not null
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}
),

q_split_by_words as (
  select
    market_id,
    split(question, ' ') as words
  from markets
),

q_common_words as (
  select
    q.market_id,
    m.q_count,
    u.word,
    count(u.word)
  from q_split_by_words q
    cross join unnest(q.words) as u(word)
    inner join (
      select market_id, count(question_id) as q_count
      from markets
      group by market_id
    ) m on q.market_id = m.market_id
  group by 1, 2, 3
  having count(u.word) > m.q_count - 1
),

q_keywords as (
  select
    m.market_id,
    m.question_id,
    m.question,
    --array_agg(cw.word) as common_words,
    concat_ws(' ', array_except(split(m.question, ' '), array_agg(cw.word))) as keyword
  from markets m
    inner join q_common_words cw on m.market_id = cw.market_id
  group by 1, 2, 3
)

select
  c.block_time,
  c.block_number,
  c.question_id,
  qk.question,
  c.condition_id,
  c.condition_token,
  c.condition_status,
  qk.keyword,
  concat(qk.keyword, ' wins - ', c.condition_status) as condition,
  c.outcome_slot_count,
  c.oracle,
  c.evt_index,
  c.tx_hash
from conditions c
  left join q_keywords qk on c.question_id = qk.question_id
