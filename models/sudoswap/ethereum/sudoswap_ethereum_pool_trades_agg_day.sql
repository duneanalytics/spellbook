{{ config(
        alias = 'pool_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["niftytable","0xRob"]\') }}'
        )
}}

{% set project_start_date = '2022-04-23' %}


SELECT
    block_date as day,
    CASE
      WHEN trade_category = 'Sell' THEN buyer
      ELSE seller
    END AS pool_address,
    sum(amount_original) AS eth_volume,
    sum(amount_usd) AS usd_volume,
    sum(number_of_items) AS nfts_traded,
    sum(pool_fee_amount) AS pool_fee_volume_eth,
    coalesce(sum(pool_fee_amount) filter (where trade_category='Sell'),0) AS pool_fee_bid_volume_eth,
    coalesce(sum(pool_fee_amount) filter (where trade_category='Buy'),0) AS pool_fee_ask_volume_eth,
    sum(platform_fee_amount) AS platform_fee_volume_eth,
    sum(
      CASE
        WHEN trade_category = 'Sell' THEN -1 * amount_original
        ELSE (amount_original-platform_fee_amount)
      END
    ) AS eth_change_trading,
    sum(
      CASE
        WHEN trade_category = 'Sell' THEN number_of_items
        ELSE -1 * number_of_items
      END
    ) AS nft_change_trading
FROM {{ ref('sudoswap_ethereum_events') }}
    {% if not is_incremental() %}
    WHERE block_date >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc("day", now() - interval '1 week')
    {% endif %}
GROUP BY 1,2
;
