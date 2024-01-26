{%
  macro lending_aave_v1_compatible_flashloans(
    blockchain,
    project,
    version,
    aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    native_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    project_decoded_as = 'aave'
  )
%}

with

src_LendingPool_evt_FlashLoan as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_FlashLoan') }}
  where cast(_amount as double) > 0
  {% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_flashloans as (
  select
    _target as recipient,
    cast(_amount as double) as amount,
    cast(_totalFee as double) as fee,
    case when _reserve = {{ aave_mock_address }} then {{ native_token_address }} else _reserve end as token_address,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_FlashLoan
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  recipient,
  amount,
  fee,
  token_address,
  contract_address,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_index
from base_flashloans

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_v2_compatible_flashloans(
    blockchain,
    project,
    version,
    aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    native_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    project_decoded_as = 'aave_v2'
  )
%}

with

src_LendingPool_evt_FlashLoan as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_FlashLoan') }}
  where cast(amount as double) > 0
  {% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_flashloans as (
  select
    target as recipient,
    cast(amount as double) as amount,
    cast(premium as double) as fee,
    case when asset = {{ aave_mock_address }} then {{ native_token_address }} else asset end as token_address,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_FlashLoan
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  recipient,
  amount,
  fee,
  token_address,
  contract_address,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_index
from base_flashloans

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_v3_compatible_flashloans(
    blockchain,
    project,
    version,
    aave_mock_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    native_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    project_decoded_as = 'aave_v3',
    decoded_contract_name = 'Pool'
  )
%}

with

src_LendingPool_evt_FlashLoan as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_FlashLoan') }}
  where cast(amount as double) > 0
  {% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_flashloans as (
  select
    target as recipient,
    cast(amount as double) as amount,
    cast(premium as double) as fee,
    case when asset = {{ aave_mock_address }} then {{ native_token_address }} else asset end as token_address,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_FlashLoan
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  recipient,
  amount,
  fee,
  token_address,
  contract_address,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_index
from base_flashloans

{% endmacro %}
