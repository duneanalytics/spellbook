{%
  macro lending_morpho_v1_compatible_flashloans(
    blockchain = '',
    project = 'morpho',
    version = '1',
    decoded_project = 'morpho_blue'
  )
%}

with

src_MorphoBlue_evt_FlashLoan as (
  select *
  from {{ source(decoded_project ~ '_' ~ blockchain, 'MorphoBlue_evt_FlashLoan') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_flashloans as (
  select
    caller as recipient,
    assets as amount,
    cast(0 as uint256) as fee,
    token as token_address,
    contract_address as project_contract_address,
    date_trunc('month', evt_block_time) as block_month,
    evt_block_time as block_time,
    evt_block_number as block_number,
    evt_tx_hash as tx_hash,
    evt_index
  from src_MorphoBlue_evt_FlashLoan
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  recipient,
  amount,
  fee,
  token_address,
  contract_address as project_contract_address,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_index
from base_flashloans

{% endmacro %}
