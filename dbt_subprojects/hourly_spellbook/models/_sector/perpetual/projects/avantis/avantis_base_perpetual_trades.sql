{{ config(
    schema = 'avantis_perpetual_trades',
    alias = 'perpetual_trades',
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                    spell_type = "project",
                                    spell_name = "avantis",
                                    contributors = \'["princi"]\') }}'
        )
}}

{% set avantis_base_perpetual_trade_models = [
    ref('avantis_v1_base_perpetual_trades')
] %}

WITH transactions_filtered AS (
    SELECT
      hash,
      block_number,
      "from",
      "to",
      block_time
    FROM 
      {{ source('base', 'transactions') }}
    WHERE
      {% if is_incremental() %}
        {{ incremental_predicate('block_time') }}
      {% else %}
        block_time >= TIMESTAMP '{{ project_start_date }}'
      {% endif %}
),

all_perpetual_trades AS (
    {% for avantis_perpetual_trades in avantis_base_perpetual_trade_models %}
    SELECT
        blockchain,
        block_date,
        block_month,
        block_time,
        virtual_asset,
        underlying_asset,
        market,
        market_address,
        volume_usd,
        fee_usd,
        margin_usd,
        trade,
        project,
        version,
        frontend,
        trader,
        volume_raw,
        tx_hash,
        tx_from,
        tx_to,
        evt_index
    FROM {{ avantis_perpetual_trades }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

SELECT
    perps.blockchain,
    perps.block_date,
    perps.block_month,
    perps.block_time,
    perps.virtual_asset,
    perps.underlying_asset,
    perps.market,
    perps.market_address,
    perps.volume_usd,
    perps.fee_usd,
    perps.margin_usd,
    perps.trade,
    perps.project,
    perps.version,
    perps.frontend,
    perps.trader,
    perps.volume_raw,
    perps.tx_hash,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    perps.evt_index
FROM all_perpetual_trades AS perps
INNER JOIN transactions_filtered AS tx
    ON perps.tx_hash = tx.hash;
