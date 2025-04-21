{%
  macro lending_aave_v3_compatible_liquidity_pool(
    blockchain,
    project,
    version,
    project_decoded_as = 'aave_v3',
    decoded_contract_name = 'Pool'
  )
%}

with

reserve_data as (
  select
    block_time,
    block_date,
    block_number,
    token_address,
    token_symbol,
    liquidity_index,
    variable_borrow_index,
    project_contract_address,
    evt_index,
    tx_hash
  from {{ ref('lending_' ~ blockchain ~ '_base_reserve') }}
    where project = '{{ project }}'
      and version = '{{ version }}'
),

reserve_data_daily as (
  select block_date, token_address, token_symbol, liquidity_index, variable_borrow_index
  from (
    select
      block_date,
      token_address,
      token_symbol,
      liquidity_index,
      variable_borrow_index,
      row_number() over (partition by block_date, token_address order by block_number desc, evt_index desc) as rn
    from reserve_data
  ) t
  where rn = 1
),

supplied as (
  select
    cast(s.block_time as date) as block_date,
    coalesce(s.on_behalf_of, s.depositor) as depositor,
    s.token_address,
    rd.token_symbol,
    sum(s.amount / rd.liquidity_index * power(10, 27)) as atoken_amount
  from {{ ref('lending_' ~ blockchain ~ '_base_supply') }} s
    inner join reserve_data rd
       on rd.block_number = s.block_number
      and rd.evt_index < s.evt_index
      and rd.tx_hash = s.tx_hash
      and rd.token_address = s.token_address
  where s.blockchain = '{{ blockchain }}'
    and s.project = '{{ project }}'
    and s.version = '{{ version }}'
  group by 1, 2, 3, 4
),

scaled_supplies as (
  select
    rd.block_date,
    rd.token_address,
    rd.token_symbol,
    wa.depositor,
    sum(s.atoken_amount) over (partition by wa.depositor, rd.token_address order by rd.block_date) * rd.liquidity_index / power(10, 27) as supplied_amount
  from reserve_data_daily rd
    cross join (select distinct depositor from supplied) as wa
    left join supplied s on rd.block_date = s.block_date and rd.token_address = s.token_address and wa.depositor = s.depositor
),

borrowed as (
  select
    cast(b.block_time as date) as block_date,
    coalesce(b.on_behalf_of, b.borrower) as borrower,
    b.token_address,
    rd.token_symbol,
    sum(b.amount / rd.variable_borrow_index * power(10, 27)) as atoken_amount
  from {{ ref('lending_' ~ blockchain ~ '_base_borrow') }} b
    inner join reserve_data rd
       on rd.block_number = b.block_number
      and rd.evt_index < b.evt_index
      and rd.tx_hash = b.tx_hash
      and rd.token_address = b.token_address
  where b.blockchain = '{{ blockchain }}'
    and b.project = '{{ project }}'
    and b.version = '{{ version }}'
  group by 1, 2, 3, 4
),

scaled_borrows as (
  select
    rd.block_date,
    rd.token_address,
    rd.token_symbol,
    wa.borrower,
    sum(b.atoken_amount) over (partition by wa.borrower, rd.token_address order by rd.block_date) * rd.variable_borrow_index / power(10, 27) as borrowed_amount
  from reserve_data_daily rd
    cross join (select distinct borrower from borrowed) as wa
    left join borrowed b on rd.block_date = b.block_date and rd.token_address = b.token_address and wa.borrower = b.borrower
),

wallet_addresses as (
  select wallet_address, token_address, date_add('day', -1, min(first_block_date)) as first_block_date
  from (
    select depositor as wallet_address, token_address, min(block_date) as first_block_date from supplied group by 1, 2
    union all
    select borrower as wallet_address, token_address, min(block_date) as first_block_date from borrowed group by 1, 2
  ) t
  group by 1, 2
),

day_sequence as (
  select wallet_address, token_address, cast(d.seq_date as date) as block_date
  from (
    select wallet_address, token_address, sequence(first_block_date, current_date, interval '1' day) as days
    from wallet_addresses
  ) as days_seq
    cross join unnest(days) as d(seq_date)
),

daily_running_totals as (
  select
    ds.block_date,
    ds.wallet_address,
    ds.token_address,
    coalesce(s.token_symbol, b.token_symbol) as token_symbol,
    coalesce(s.supplied_amount, 0) as supplied_amount,
    -1 * coalesce(b.borrowed_amount, 0) as borrowed_amount
  from day_sequence ds
    left join scaled_supplies s on ds.block_date = s.block_date and ds.token_address = s.token_address and ds.wallet_address = s.depositor
    left join scaled_borrows b on ds.block_date = b.block_date and ds.token_address = b.token_address and ds.wallet_address = b.borrower
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  block_date,
  wallet_address,
  token_address,
  token_symbol,
  supplied_amount,
  borrowed_amount
from daily_running_totals 

{% endmacro %}
