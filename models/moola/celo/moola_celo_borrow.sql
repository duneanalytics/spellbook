{{
    config(
        
        schema = 'moola_celo',
        alias = 'borrow',
        partition_by = ['evt_block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['transaction_type', 'evt_tx_hash', 'evt_index'],
        post_hook = '{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "moola",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  cast(date_trunc('month', borrow.evt_block_time) as date) as evt_block_month,
  borrow.transaction_type,
  borrow.loan_type,
  erc20.symbol,
  borrow.token_address,
  borrow.borrower,
  borrow.repayer,
  borrow.liquidator,
  borrow.amount / power(10, erc20.decimals) as amount,
  (borrow.amount / power(10, p.decimals)) * p.price as amount_usd,
  borrow.evt_tx_hash,
  borrow.evt_index,
  borrow.evt_block_time,
  borrow.evt_block_number
from (
    select
      'borrow' as transaction_type,
      case
        when borrowRateMode = uint256 '1' then 'stable'
        when borrowRateMode = uint256 '2' then 'variable'
      end as loan_type,
      reserve as token_address,
      user as borrower, 
      cast(null as varbinary) as repayer,
      cast(null as varbinary) as liquidator,
      cast(amount as double) as amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_Borrow') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    union all 
    select
      'repay' as transaction_type,
      null as loan_type,
      reserve as token_address,
      user as borrower,
      repayer as repayer,
      cast(null as varbinary) as liquidator,
      -1 * cast(amount as double) as amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_Repay') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    union all
    select
      'borrow_liquidation' as transaction_type,
      null as loan_type,
      debtAsset as token_address,
      user as borrower,
      liquidator as repayer,
      liquidator  as liquidator,
      -1 * cast(debtToCover as double) as amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_LiquidationCall') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
  ) borrow
  left join {{ ref('tokens_celo_erc20') }} erc20 on borrow.token_address = erc20.contract_address
  left join {{ source('prices', 'usd') }} p on p.blockchain = 'celo'
    and date_trunc('minute', borrow.evt_block_time) = p.minute
    and borrow.token_address = p.contract_address
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}
