{{ config(
        alias = alias('pool_settings_latest'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["0xRob"]\') }}'
        )
}}

{% set project_start_date = '2022-04-23' %}

with
  latest_pool_fee as (
    SELECT
        pool_address
        , pool_fee
        , update_time
        FROM (
            SELECT
                contract_address as pool_address
                ,newFee as pool_fee
                ,evt_block_time as update_time
                ,row_number() over (partition by contract_address order by evt_block_number desc, tx.index desc) as ordering
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_FeeUpdate') }} evt
            INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.block_time = evt.evt_block_time
            AND tx.hash = evt.evt_tx_hash
            {% if not is_incremental() %}
            AND tx.block_time >= '{{project_start_date}}'
            AND evt.evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) foo
    WHERE ordering = 1
)

, latest_delta as (
    SELECT
        pool_address
        , delta
        , update_time
        FROM (
            SELECT
                contract_address as pool_address
                ,newDelta/1e18 as delta
                ,evt_block_time as update_time
                ,row_number() over (partition by contract_address order by evt_block_number desc, tx.index desc) as ordering
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_DeltaUpdate') }} evt
            INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.block_time = evt.evt_block_time
            AND tx.hash = evt.evt_tx_hash
            {% if not is_incremental() %}
            AND tx.block_time >= '{{project_start_date}}'
            AND evt.evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) foo
    WHERE ordering = 1
)

, latest_spot_price as (
    SELECT
        pool_address
        , spot_price
        , update_time
        FROM (
            SELECT
                contract_address as pool_address
                ,newSpotPrice/1e18 as spot_price
                ,evt_block_time as update_time
                ,row_number() over (partition by contract_address order by evt_block_number desc, tx.index desc) as ordering
            FROM {{ source('sudo_amm_ethereum','LSSVMPair_general_evt_SpotPriceUpdate') }} evt
            INNER JOIN {{ source('ethereum','transactions') }} tx ON tx.block_time = evt.evt_block_time
            AND tx.hash = evt.evt_tx_hash
            {% if not is_incremental() %}
            AND tx.block_time >= '{{project_start_date}}'
            AND evt.evt_block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc("day", now() - interval '1 week')
            AND evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        ) foo
    WHERE ordering = 1
)

, latest_settings as (
    select
        coalesce(t1.pool_address, t2.pool_address, t3.pool_address) as pool_address
        ,pool_fee
        ,delta
        ,spot_price
        ,coalesce(t1.update_time, t2.update_time, t3.update_time) as latest_update_time
    from latest_spot_price t1
    full join latest_delta t2 on t1.pool_address = t2.pool_address
    full join latest_pool_fee t3 on t1.pool_address = t3.pool_address or t2.pool_address = t3.pool_address
)

, initial_settings as (
    SELECT
      pool_address,
      bonding_curve,
      spot_price,
      delta,
      pool_fee,
      creation_block_time
    FROM
      {{ ref('sudoswap_ethereum_pool_creations') }}
    {% if is_incremental() %}
    WHERE creation_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

-- incremental update:
-- we need to backfill columns from the existing data in order to have full rows
{% if is_incremental() %}
, full_settings_backfilled as (
    select * from(
        select
         coalesce(t1.pool_address,t3.pool_address) as pool_address
        ,coalesce(t2.bonding_curve, t3.bonding_curve) as bonding_curve
        ,coalesce(t1.pool_fee, t2.pool_fee, t3.pool_fee) as pool_fee
        ,coalesce(t1.delta, t2.delta, t3.delta) as delta
        ,coalesce(t1.spot_price, t2.spot_price, t3.spot_price) as spot_price
        ,coalesce(t1.latest_update_time,t3.creation_block_time) as latest_update_time
        from latest_settings t1
        full outer join initial_settings t3
            ON t1.pool_address = t3.pool_address
        left join {{ this }} t2
            ON t1.pool_address = t2.pool_address
    ) foo
    where bonding_curve is not null --temp hack to exclude updates form erc20 pools
)
{% endif %}


-- This happens on a full refresh, no backfill necesarry.
{% if not is_incremental() %}
, full_settings_backfilled as (
    select
     coalesce(new.pool_address,old.pool_address) as pool_address
    ,coalesce(old.bonding_curve) as bonding_curve
    ,coalesce(new.pool_fee,old.pool_fee) as pool_fee
    ,coalesce(new.delta, old.delta) as delta
    ,coalesce(new.spot_price,old.spot_price) as spot_price
    ,coalesce(new.latest_update_time,old.creation_block_time) as latest_update_time
    from initial_settings old
    left join latest_settings new
    ON old.pool_address = new.pool_address
)
{% endif %}


select * from full_settings_backfilled
;
