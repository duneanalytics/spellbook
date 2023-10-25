{{
    config(
        
        schema = 'moola_celo',
        alias = 'flashloans',
        partition_by = ['evt_block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_tx_hash', 'evt_index'],
        post_hook = '{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "moola",
                                    \'["tomfutago"]\') }}'
    )
}}

with flashloans as (
  select
    flash.evt_block_time,
    flash.evt_block_number,
    cast(flash.amount as double) as amount,
    flash.evt_tx_hash,
    flash.evt_index,
    cast(flash.premium as double) as fee,
    flash.asset as token_address,
    erc20.symbol as symbol,
    erc20.decimals as currency_decimals,
    flash.target as recipient,
    flash.contract_address
  from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_FlashLoan') }} flash
    left join {{ ref('tokens_celo_erc20') }} erc20 on erc20.contract_address = flash.asset
  where cast(flash.amount as double) > 0
    {% if is_incremental() %}
    and {{ incremental_predicate('flash.evt_block_time') }}
    {% endif %}
)

select
  cast(date_trunc('month', flash.evt_block_time) as date) as evt_block_month,
  flash.evt_block_time,
  flash.evt_block_number,
  flash.amount / power(10, flash.currency_decimals) as amount,
  p.price * flash.amount / power(10, flash.currency_decimals) as amount_usd,
  flash.evt_tx_hash,
  flash.evt_index,
  flash.fee / power(10, flash.currency_decimals) as fee,
  flash.token_address,
  flash.symbol,
  flash.recipient,
  flash.contract_address
from flashloans flash
  left join {{ source('prices','usd') }} p on p.blockchain = 'celo'  
    and flash.token_address = p.contract_address
    and date_trunc('minute', flash.evt_block_time) = p.minute
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}
