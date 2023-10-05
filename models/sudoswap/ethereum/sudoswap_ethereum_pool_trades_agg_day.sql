{{ config(
        alias = alias('pool_trades'),
        materialized = 'incremental',
        schema = 'sudoswap_ethereum',
        tags = ['dunesql'],
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
    sum(price_raw/1e18) AS eth_volume,
    sum(usd.price*(price_raw/1e18)) AS usd_volume,
    sum(nft_amount) AS nfts_traded,
    sum(pool_fee_amount_raw/1e18) AS pool_fee_volume_eth,
    coalesce(sum(pool_fee_amount_raw/1e18) filter (where trade_category='Sell'),0) AS pool_fee_bid_volume_eth,
    coalesce(sum(pool_fee_amount_raw/1e18) filter (where trade_category='Buy'),0) AS pool_fee_ask_volume_eth,
    sum(platform_fee_amount_raw/1e18) AS platform_fee_volume_eth,
    sum(
      CASE
        WHEN trade_category = 'Sell' THEN -1 * cast(price_raw as int256)/1e18
        ELSE cast(price_raw-platform_fee_amount_raw as int256)/1e18
      END
    ) AS eth_change_trading,
    sum(
      CASE
        WHEN trade_category = 'Sell' THEN cast(nft_amount as int256)
        ELSE -1 * cast(nft_amount as int256)
      END
    ) AS nft_change_trading
FROM {{ ref('sudoswap_ethereum_base_trades') }} t
LEFT JOIN {{ ref('prices_usd_forward_fill') }} usd
ON usd.blockchain = null and usd.symbol = 'ETH'
    AND usd.minute = date_trunc('minute',t.block_time)
    {% if not is_incremental() %}
    AND minute >= TIMESTAMP '{{project_start_date}}'
    {% else %}
    AND minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
{% if not is_incremental() %}
WHERE block_date >= TIMESTAMP '{{project_start_date}}'
{% else %}
WHERE block_date >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY 1,2
