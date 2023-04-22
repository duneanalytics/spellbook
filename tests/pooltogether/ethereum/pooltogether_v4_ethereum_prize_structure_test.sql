with
  unit_test as (
    select
      case
        when test.network = actual.network then true
        else false
      end as network_test,
      case
        when test.draw_id = actual.draw_id then true
        else false
      end as draw_id_test,
      case
        when test.bit_range = actual.bit_range then true
        else false
      end as bit_range_test,
      case
        when test.tiers1 = actual.tiers1 then true
        else false
      end as tiers1_test,
      case
        when test.dpr = actual.dpr then true
        else false
      end as dpr_test,
      case
        when test.prize = actual.prize then true
        else false
      end as prize_test
    from
      {{ ref('pooltogether_v4_ethereum_prize_structure') }} as actual
    inner join
      {{ ref('pooltogether_v4_ethereum_prize_structure_seed') }} as test
    on lower(actual.tx_hash) = lower(test.tx_hash)
  )
select
  *
from
  unit_test
where
  network_test = false
  or draw_id_test = false
  or bit_range_test = false
  or tiers1_test = false
  or dpr_test = false
  or prize_test = false