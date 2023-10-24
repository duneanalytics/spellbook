{{
    config(
        
        schema = 'moola_celo',
        alias = 'supply',
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
  cast(date_trunc('month', deposit.evt_block_time) as date) as evt_block_month,
  deposit.transaction_type,
  erc20.symbol,
  deposit.token_address,
  deposit.depositor,
  deposit.withdrawn_to,
  deposit.liquidator,
  deposit.amount / power(10, erc20.decimals) as amount,
  (deposit.amount / power(10, p.decimals)) * p.price as amount_usd,
  deposit.evt_tx_hash,
  deposit.evt_index,
  deposit.evt_block_time,
  deposit.evt_block_number
from (
    select
      'deposit' as transaction_type,
      reserve as token_address,
      user as depositor,
      cast(null as varbinary) as withdrawn_to,
      cast(null as varbinary) as liquidator,
      cast(amount as double) as amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_Deposit') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    union all
    select
      'withdraw' as transaction_type,
      reserve as token_address,
      user as depositor,
      to as withdrawn_to,
      cast(null as varbinary) as liquidator,
      -1 * cast(amount as double) as amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_Withdraw') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    union all
    select
      'deposit_liquidation' as transaction_type,
      collateralAsset as token_address,
      user as depositor,
      liquidator as withdrawn_to,
      liquidator as liquidator,
      -1 * cast(liquidatedCollateralAmount as double) as amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source('moolainterestbearingmoo_celo', 'LendingPool_evt_LiquidationCall') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
  ) deposit
  left join {{ ref('tokens_celo_erc20') }} erc20 on deposit.token_address = erc20.contract_address
  left join {{ source('prices', 'usd') }} p on p.blockchain = 'celo'
    and date_trunc('minute', deposit.evt_block_time) = p.minute
    and deposit.token_address = p.contract_address
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}
